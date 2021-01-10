package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"path/filepath"
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
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/util/homedir"
)

var ctx = context.Background()

var rdb *redis.Client

var restConfig *rest.Config

var clientset *kubernetes.Clientset

var varnishNamespace, varnishInstanceName, subscribeChannel *string

func init() {
	var kubeconfig *string
	redisHost := flag.String("redisHost", "127.0.0.1", "ip address or hostname of redis instance")
	redisPort := flag.Int("redisPort", 6379, "port listened of redis instance")
	redisPassword := flag.String("redisPassword", "", "authentication password of redis")
	redisDB := flag.Int("redisDB", 0, "db index of redis")
	subscribeChannel = flag.String("subscribeChannel", "cleanCache", "which channel will be subscribed")
	varnishNamespace = flag.String("varnishNamespace", "varnish", "kubernetes namespace of varnish")
	varnishInstanceName = flag.String("varnishServiceName", "cache-service", "kubernetes container name of varnish")
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	rdb = redis.NewClient(&redis.Options{
		Addr:     *redisHost + ":" + strconv.Itoa(*redisPort),
		Password: *redisPassword, // no password set
		DB:       *redisDB,       // use default DB
	})

	var err error

	restConfig, err = rest.InClusterConfig()

	if err == nil {
		clientset, err = kubernetes.NewForConfig(restConfig)

		if err != nil {
			panic(err.Error())
		}
	} else {
		// use the current context in kubeconfig
		restConfig, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)

		if err != nil {
			panic(err.Error())
		}

		// create the clientset
		clientset, err = kubernetes.NewForConfig(restConfig)
		if err != nil {
			panic(err.Error())
		}
	}
}

func main() {
	pubsub := rdb.Subscribe(ctx, *subscribeChannel)

	defer pubsub.Close()

	// Wait for confirmation that subscription is created before publishing anything.
	_, err := pubsub.Receive(ctx)
	if err != nil {
		panic(err)
	} else {
		fmt.Printf("Sucessful Subscribe to Channel - %s\n", *subscribeChannel)
	}

	// Go channel which receives messages.
	ch := pubsub.Channel()

	// Consume messages.
	for msg := range ch {
		kubeDNS, ok := getKubeDNS(clientset, msg.Payload)

		if !ok {
			fmt.Println("Service Not Found")
			continue
		}

		varnishPodList, err := clientset.CoreV1().Pods(*varnishNamespace).List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			panic(err.Error())
		}

		wg := new(sync.WaitGroup)

		for _, varnishPod := range varnishPodList.Items {

			Container, ok := getContainer(*varnishInstanceName, varnishPod)

			if !ok {
				continue
			}
			wg.Add(1)
			go execInPod(restConfig, clientset, varnishPod.GetNamespace(), varnishPod.GetName(), "varnishadm ban req.http.host == "+kubeDNS, Container.Name, wg)
		}

		wg.Wait()
		fmt.Printf("Sucessful clean cache of %s\n", msg.Payload)
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
