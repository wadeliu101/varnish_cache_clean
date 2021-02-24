package main

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/go-redis/redis/v8"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

var ctx = context.Background()

var cfg *Config

var rdb *redis.Client

var restConfig *rest.Config

var clientset *kubernetes.Clientset

var varnishNamespace, varnishServiceName, varnishContainerName, subscribeChannel string

func init() {
	configfile, err := ParseFlags()

	if err != nil {
		fmt.Println(err)
		panic("failed to parse configfile")
	}

	cfg, err = NewConfig(configfile)

	if err != nil {
		fmt.Println(err)
		panic("failed to parse configfile")
	}

	rdb = redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Host + ":" + strconv.Itoa(cfg.Redis.Port),
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})

	restConfig, err = rest.InClusterConfig()

	clientset, err = kubernetes.NewForConfig(restConfig)
	if err != nil {
		panic(err.Error())
	}

	varnishNamespace = cfg.Varnish.Namespace
	varnishServiceName = cfg.Varnish.ServiceName
	varnishContainerName = cfg.Varnish.ContainerName
	subscribeChannel = cfg.Channel
}

func main() {
	pubsub := rdb.Subscribe(ctx, subscribeChannel)

	defer pubsub.Close()

	// Wait for confirmation that subscription is created before publishing anything.
	_, err := pubsub.Receive(ctx)
	if err != nil {
		panic(err)
	} else {
		fmt.Printf("Sucessful Subscribe to Channel - %s\n", subscribeChannel)
	}

	// Go channel which receives messages.
	ch := pubsub.Channel()

	// Consume messages.
	for msg := range ch {
		var cmd string
		if msg.Payload == "configReload" {
			cmd = "varnishreload /etc/varnish/default.vcl"
		} else {
			kubeDNS, ok := getKubeDNS(clientset, msg.Payload)
			if !ok {
				fmt.Println("Service Not Found")
				continue
			}
			cmd = "varnishadm ban req.http.host == " + kubeDNS
		}

		varnishPodList, err := clientset.CoreV1().Pods(varnishNamespace).List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			panic(err.Error())
		}

		wg := new(sync.WaitGroup)

		for _, varnishPod := range varnishPodList.Items {

			Container, ok := getContainer(varnishContainerName, varnishPod)

			if !ok {
				continue
			}
			wg.Add(1)
			go execInPod(restConfig, clientset, varnishPod.Namespace, varnishPod.Name, cmd, Container.Name, wg)
		}

		wg.Wait()

		if msg.Payload == "configReload" {
			fmt.Println("Sucessful reload config")
		} else {
			fmt.Printf("Sucessful clean cache of %s\n", msg.Payload)
		}
	}
}

func getKubeDNS(clientset *kubernetes.Clientset, service string) (string, bool) {
	kubeServiceList, err := clientset.CoreV1().Services("").List(context.TODO(), metav1.ListOptions{})

	if err != nil {
		panic(err.Error())
	}

	for _, kubeService := range kubeServiceList.Items {
		if kubeService.Name == service {
			return kubeService.GetName() + "." + kubeService.GetNamespace() + ".svc.cluster.local", true
		}
	}

	return "", false
}

func getContainer(containerName string, pod v1.Pod) (v1.Container, bool) {
	for index, container := range pod.Spec.Containers {
		if container.Name == containerName && pod.Status.ContainerStatuses[index].Ready {
			return container, true
		}
	}

	return v1.Container{}, false
}

func execInPod(restConfig *rest.Config, clientset *kubernetes.Clientset, namespace, podName, command, containerName string, wg *sync.WaitGroup) {
	defer wg.Done()
	cmd := []string{
		"/bin/sh",
		"-c",
		command,
	}
	req := clientset.CoreV1().RESTClient().Post().Resource("pods").Name(podName).Namespace(namespace).SubResource("exec").VersionedParams(
		&v1.PodExecOptions{
			Container: containerName,
			Command:   cmd,
			Stdin:     false,
			Stdout:    true,
			Stderr:    true,
			TTY:       false,
		},
		scheme.ParameterCodec,
	)

	var stdout, stderr bytes.Buffer
	exec, err := remotecommand.NewSPDYExecutor(restConfig, "POST", req.URL())
	if err != nil {
		panic(err.Error())
	}
	err = exec.Stream(remotecommand.StreamOptions{
		Stdin:  nil,
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if err != nil {
		panic(err.Error())
	}
	fmt.Println(strings.TrimSpace(stdout.String()), strings.TrimSpace(stderr.String()))
}
