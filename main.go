package main

import (
	"os"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"

	wfclientset "github.com/argoproj/argo/pkg/client/clientset/versioned"
	wfextvv1alpha1 "github.com/argoproj/argo/pkg/client/informers/externalversions/workflow/v1alpha1"
)

// retrieve the Kubernetes cluster client from outside of the cluster
func getKubernetesClient() (kubernetes.Interface, wfclientset.Interface) {
	// construct the path to resolve to `~/.kube/config`
	kubeConfigPath := os.Getenv("HOME") + "/.kube/config"

	// create the config from the path
	config, err := clientcmd.BuildConfigFromFlags("", kubeConfigPath)
	if err != nil {
		log.Fatalf("getClusterConfig: %v", err)
	}

	// generate the client based off of the config
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("getClusterConfig: %v", err)
	}

	wfClient, err := wfclientset.NewForConfig(config)
	if err != nil {
		log.Fatalf("getClusterConfig: %v", err)
	}

	log.Info("Successfully constructed k8s client")
	return client, wfClient
}

// main code path
func main() {
	// get the Kubernetes client for connectivity
	client, wfClient := getKubernetesClient()

	// retrieve our custom resource informer which was generated from
	// the code generator and pass it the custom resource client, specifying
	// we should be looking through all namespaces for listing and watching
	informer := wfextvv1alpha1.NewWorkflowInformer(
		wfClient,
		meta_v1.NamespaceAll,
		0,
		cache.Indexers{},
	)

	// create a new queue so that when the informer gets a resource that is either
	// a result of listing or watching, we can add an idenfitying key to the queue
	// so that it can be handled in the handler
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	// add event handlers to handle the three types of events for resources:
	//  - adding new resources
	//  - updating existing resources
	//  - deleting resources
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			/*
				xType := fmt.Sprintf("%T", obj)
				if xType == "*v1.PrivateDataBlock" {

					// perform initializations
					newBlock := obj.(*v1.PrivateDataBlock)

					blockHandler := framework.NewBlockHandler(newBlock)

					blockCopy := blockHandler.Snapshot()

					blockCopy.Status.AvailableBudget.Epsilon = blockCopy.Spec.Budget.Epsilon
					blockCopy.Status.AvailableBudget.Delta = blockCopy.Spec.Budget.Delta
					blockCopy.Status.LockedBudgetMap = make(map[string]v1.PrivacyBudget)

					res, err := datablockClient.ColumbiaV1().PrivateDataBlocks(blockCopy.Namespace).UpdateStatus(blockCopy)

					if res == nil || err != nil {

						fmt.Print("error at updateStatus: ")
						fmt.Print(err)
					}

				} else {

					fmt.Println("Unexpected type at AddFunc: " + xType)
				}
			*/

			// convert the resource object into a key (in this case
			// we are just doing it in the format of 'namespace/name')
			key, err := cache.MetaNamespaceKeyFunc(obj)

			log.Infof("Add datablock: %s", key)
			if err == nil {
				// add the key to the queue for the handler to get
				queue.Add(key)
			}

		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			/*
				updatedObjType := fmt.Sprintf("%T", newObj)
				oldObjType := fmt.Sprintf("%T", oldObj)

				if updatedObjType == "*v1.PrivateDataBlock" && oldObjType == "*v1.PrivateDataBlock" {

					updatedBlock := newObj.(*v1.PrivateDataBlock)
					oldBlock := oldObj.(*v1.PrivateDataBlock)

					equivalence := reflect.DeepEqual(updatedBlock.Status.LockedBudgetMap, oldBlock.Status.LockedBudgetMap)

					// the two maps are different, an update has been made this LockedBudgetMap
					// so the Pod annotations must be updated
					if !equivalence {

						for key, val := range updatedBlock.Status.LockedBudgetMap {

							newBudget := val
							oldBudget := oldBlock.Status.LockedBudgetMap[key]

							// check if this is one of the differing entries
							if newBudget.Delta != oldBudget.Delta || newBudget.Epsilon != oldBudget.Epsilon {

								pod, err := client.CoreV1().Pods(updatedBlock.Namespace).Get(context.TODO(), key, meta_v1.GetOptions{})

								if err != nil {

									fmt.Println("Error in UpdateFunc from  Pods.Get(): ")
									fmt.Print(err)
								} else {

									podHandler := framework.NewPodHandler(pod)

									podCopy := podHandler.Snapshot()
									annotations := podCopy.Annotations["dp-blocks"]

									if strings.Contains(annotations, updatedBlock.Name) {

										blockMappings := strings.Split(annotations, ";")

										newMappings := ""
										for _, mapping := range blockMappings {

											if strings.Contains(mapping, updatedBlock.Name) {

												indexOfEpsilon := strings.Index(mapping, "Epsilon:") + 8
												indexOfEpsilonDelimeter := strings.Index(mapping, ",")
												indexOfDelta := strings.Index(mapping, "Delta:") + 6
												indexOfDeltaDelimter := strings.Index(mapping, ";")

												originalEpsilon, err := strconv.ParseFloat(mapping[indexOfEpsilon:indexOfEpsilonDelimeter], 64)

												if err != nil {

													fmt.Println("Error parsing float in UpdateFunc")
												}
												originalDelta, err := strconv.ParseFloat(mapping[indexOfDelta:indexOfDeltaDelimter], 64)

												if err != nil {

													fmt.Println("Error parsing float in UpdateFunc")
												}

												newEpsilon := originalEpsilon + newBudget.Epsilon
												newDelta := originalDelta + newBudget.Delta

												newEpsilonStr := fmt.Sprintf("%f", newEpsilon)
												newDeltaStr := fmt.Sprintf("%f", newDelta)
												newMapping := updatedBlock.Name + "->Epsilon:" + newEpsilonStr + ",Delta:" + newDeltaStr
												newMappings += newMapping + ";"
											} else {

												newMappings += mapping + ";"
											}
										}

										podCopy.Annotations["dp-blocks"] = newMappings
									} else {

										epsilonStr := fmt.Sprintf("%f", newBudget.Epsilon)
										deltaStr := fmt.Sprintf("%f", newBudget.Delta)
										podCopy.Annotations["dp-blocks"] = updatedBlock.Name + "->Epsilon:" + epsilonStr + ",Delta:" + deltaStr + ";"
									}

									_, err := client.CoreV1().Pods(podCopy.Namespace).Update(context.TODO(), podCopy, meta_v1.UpdateOptions{})

									if err != nil {

										fmt.Println("Error in UpdateFunc from Pods.Update: ")
										fmt.Print(err)
									}

								}
							}
						}

					}

					if updatedBlock.Status.AvailableBudget.Epsilon <= 0 || updatedBlock.Status.AvailableBudget.Delta <= 0 {

						// delete this block, we are going to assume that all pods have released their budget
						// considering the Epsilon and Delta are at 0

						options := meta_v1.DeleteOptions{}
						err := datablockClient.ColumbiaV1().PrivateDataBlocks(updatedBlock.Namespace).Delete(updatedBlock.Name, &options)

						if err != nil {

							log.Fatalf("Error in Delete() privatedatablock")
						}
					}

				} else {

					fmt.Println("Unexpected Object Type in UpdateFunc: " + updatedObjType)
				}
			*/
			key, err := cache.MetaNamespaceKeyFunc(newObj)
			log.Infof("Update datablock: %s", key)
			if err == nil {
				queue.Add(key)
			}
		},
		DeleteFunc: func(obj interface{}) {
			// DeletionHandlingMetaNamsespaceKeyFunc is a helper function that allows
			// us to check the DeletedFinalStateUnknown existence in the event that
			// a resource was deleted but it is still contained in the index
			//
			// this then in turn calls MetaNamespaceKeyFunc
			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			log.Infof("Delete datablock: %s", key)
			if err == nil {
				queue.Add(key)
			}
		},
	})

	// construct the Controller object which has all of the necessary components to
	// handle logging, connections, informing (listing and watching), the queue,
	// and the handler
	controller := Controller{
		logger:    log.NewEntry(log.New()),
		clientset: client,
		informer:  informer,
		queue:     queue,
		handler:   &TestHandler{},
	}

	// use a channel to synchronize the finalization for a graceful shutdown
	stopCh := make(chan struct{})
	defer close(stopCh)

	// run the controller loop to process items
	go controller.Run(stopCh)

	// use a channel to handle OS signals to terminate and gracefully shut
	// down processing
	sigTerm := make(chan os.Signal, 1)
	signal.Notify(sigTerm, syscall.SIGTERM)
	signal.Notify(sigTerm, syscall.SIGINT)
	<-sigTerm
}
