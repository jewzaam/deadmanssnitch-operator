package deadmanssnitchintegration

import (
	"context"
	"fmt"

	"github.com/openshift/deadmanssnitch-operator/config"
	deadmanssnitchv1alpha1 "github.com/openshift/deadmanssnitch-operator/pkg/apis/deadmanssnitch/v1alpha1"
	"github.com/openshift/deadmanssnitch-operator/pkg/dmsclient"
	"github.com/openshift/deadmanssnitch-operator/pkg/localmetrics"
	"github.com/openshift/deadmanssnitch-operator/pkg/utils"
	hivev1 "github.com/openshift/hive/pkg/apis/hive/v1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var log = logf.Log.WithName("controller_deadmanssnitchintegration")

const (
	deadMansSnitchAPISecretKey = "deadmanssnitch-api-key"
)

// Add creates a new DeadmansSnitchIntegration Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcileDeadmansSnitchIntegration{
		//client:    mgr.GetClient(),
		client:    mgr.GetClient(),
		scheme:    mgr.GetScheme(),
		dmsclient: dmsclient.NewClient,
	}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("deadmanssnitchintegration-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource DeadmansSnitchIntegration
	err = c.Watch(&source.Kind{Type: &deadmanssnitchv1alpha1.DeadmansSnitchIntegration{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	err = c.Watch(&source.Kind{Type: &hivev1.ClusterDeployment{}},
		&handler.EnqueueRequestsFromMapFunc{
			ToRequests: clusterDeploymentToDeadMansSnitchIntegrationsMapper{
				Client: mgr.GetClient(),
			},
		},
	)
	if err != nil {
		return err
	}

	// Watch for changes to SyncSets. If one has any ClusterDeployment owner
	// references, queue a request for all DeadMansSnitchIntegration CR that
	// select those ClusterDeployments.
	err = c.Watch(&source.Kind{Type: &hivev1.SyncSet{}},
		&handler.EnqueueRequestsFromMapFunc{
			ToRequests: ownedByClusterDeploymentToDeadMansSnitchIntegrationsMapper{
				Client: mgr.GetClient(),
			},
		},
	)
	if err != nil {
		return err
	}

	// Watch for changes to Secrets. If one has any ClusterDeployment owner
	// references, queue a request for all DeadMansSnitchIntegration CR that
	// select those ClusterDeployments.
	err = c.Watch(&source.Kind{Type: &corev1.Secret{}},
		&handler.EnqueueRequestsFromMapFunc{
			ToRequests: ownedByClusterDeploymentToDeadMansSnitchIntegrationsMapper{
				Client: mgr.GetClient(),
			},
		},
	)
	if err != nil {
		return err
	}

	return nil
}

// blank assignment to verify that ReconcileDeadmansSnitchIntegration implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileDeadmansSnitchIntegration{}

// ReconcileDeadmansSnitchIntegration reconciles a DeadmansSnitchIntegration object
type ReconcileDeadmansSnitchIntegration struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client    client.Client
	scheme    *runtime.Scheme
	dmsclient func(authToken string, collector *localmetrics.MetricsCollector) dmsclient.Client
}

// Reconcile reads that state of the cluster for a DeadmansSnitchIntegration object and makes changes based on the state read
// and what is in the DeadmansSnitchIntegration.Spec
func (r *ReconcileDeadmansSnitchIntegration) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	reqLogger := log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)
	reqLogger.Info("Reconciling DeadmansSnitchIntegration")
	// Fetch the DeadmansSnitchIntegration dmsi
	dmsi := &deadmanssnitchv1alpha1.DeadmansSnitchIntegration{}

	err := r.client.Get(context.TODO(), request.NamespacedName, dmsi)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	// set the DMS finalizer variable
	deadMansSnitchFinalizer := finalizerName(dmsi)

	dmsAPIKey, err := utils.LoadSecretData(r.client, dmsi.Spec.DmsAPIKeySecretRef.Name,
		dmsi.Spec.DmsAPIKeySecretRef.Namespace, deadMansSnitchAPISecretKey)
	if err != nil {
		return reconcile.Result{}, err
	}
	dmsc := r.dmsclient(dmsAPIKey, localmetrics.Collector)

	matchingClusterDeployments, err := r.getMatchingClusterDeployment(dmsi)
	if err != nil {
		return reconcile.Result{}, err
	}

	allClusterDeployments, err := r.getAllClusterDeployment()
	if err != nil {
		return reconcile.Result{}, err
	}

	if dmsi.DeletionTimestamp != nil {
		// trigger DMS deletion for ALL CD based on finalizer
		for _, clusterdeployment := range allClusterDeployments.Items {
			if utils.HasFinalizer(&clusterdeployment, deadMansSnitchFinalizer) {
				err = r.deleteDMSClusterDeployment(dmsi, &clusterdeployment, dmsc)
				if err != nil {
					return reconcile.Result{}, err
				}
			}
		}
		if utils.HasFinalizer(dmsi, deadMansSnitchFinalizer) {
			utils.DeleteFinalizer(dmsi, deadMansSnitchFinalizer)
			reqLogger.Info("Deleting DMSI finalizer from dmsi", "DeadMansSnitchIntegreation.Namespace", dmsi.Namespace, "DeadMansSnitchIntegreation.Name", dmsi.Name)
			err = r.client.Update(context.TODO(), dmsi)
			if err != nil {
				reqLogger.Error(err, "Error deleting Finalizer from dmsi")
				return reconcile.Result{}, err
			}
		}
		return reconcile.Result{}, nil
	}

	for _, clusterdeployment := range allClusterDeployments.Items {
		if clusterdeployment.DeletionTimestamp != nil {
			// delete DMS for any CD that are being deleted
			if utils.HasFinalizer(&clusterdeployment, deadMansSnitchFinalizer) {
				err = r.deleteDMSClusterDeployment(dmsi, &clusterdeployment, dmsc)
				if err != nil {
					return reconcile.Result{}, err
				}
			}
		} else {
			// delete DMS for any CD that has the matching finalizer but no longer matches the clusterDeploymentSelector
			if utils.HasFinalizer(&clusterdeployment, deadMansSnitchFinalizer) {
				_, found := Find(matchingClusterDeployments.Items, clusterdeployment)
				if found {
					err = r.deleteDMSClusterDeployment(dmsi, &clusterdeployment, dmsc)
					if err != nil {
						return reconcile.Result{}, err
					}
				}
			}
		}
	}

	for _, clusterdeployment := range matchingClusterDeployments.Items {
		if clusterdeployment.DeletionTimestamp != nil {
			// no action required, as this should be handled by the all function above
			continue
		}

		if !clusterdeployment.Spec.Installed {
			// Cluster isn't installed yet, continue
			continue
		}

		err = r.dmsAddFinalizer(dmsi, &clusterdeployment)
		if err != nil {
			return reconcile.Result{}, err
		}

		err = r.createSnitch(dmsi, &clusterdeployment, dmsc)
		if err != nil {
			return reconcile.Result{}, err
		}

		err = r.createSecret(dmsi, dmsc, clusterdeployment)
		if err != nil {
			return reconcile.Result{}, err
		}
		err = r.createSyncset(dmsi, clusterdeployment)
		if err != nil {
			return reconcile.Result{}, err
		}

	}
	log.Info("Reconcile of deadmanssnitch integration complete")

	return reconcile.Result{}, nil
}

// getMatchingClusterDeployment gets all ClusterDeployments matching the DMSI selector
func (r *ReconcileDeadmansSnitchIntegration) getMatchingClusterDeployment(dmsi *deadmanssnitchv1alpha1.DeadmansSnitchIntegration) (*hivev1.ClusterDeploymentList, error) {

	// need to load all CD that match via selector
	// AND all CD that currently have this DMSI's finalizer
	// and then return that result set

	// get all CD and walk through it (TODO find a better way!!)
	// (if this fails, this is OK, just ignore it)
	allClusterDeployments := &hivev1.ClusterDeploymentList{}
	err := r.client.List(context.TODO(), allClusterDeployments)

	// get the set of CD that match the selector
	selector, err := metav1.LabelSelectorAsSelector(&dmsi.Spec.ClusterDeploymentSelector)
	if err != nil {
		return nil, err
	}

	matchingClusterDeployments := &hivev1.ClusterDeploymentList{}
	listOpts := &client.ListOptions{LabelSelector: selector}
	err = r.client.List(context.TODO(), matchingClusterDeployments, listOpts)

	// and now add the CD's with the finalizer we didn't already find into the result set
	dmsiFinalizerName := finalizerName(dmsi)
	for _, cd := range allClusterDeployments.Items {
		for _, f := range cd.ObjectMeta.Finalizers {
			if f == dmsiFinalizerName {
				_, found := Find(matchingClusterDeployments.Items, cd)
				if !found {
					matchingClusterDeployments.Items = append(matchingClusterDeployments.Items, cd)
				}
			}
		}
	}

	return matchingClusterDeployments, err
}

// getAllClusterDeployment retrives all ClusterDeployments in the shard
func (r *ReconcileDeadmansSnitchIntegration) getAllClusterDeployment() (*hivev1.ClusterDeploymentList, error) {
	matchingClusterDeployments := &hivev1.ClusterDeploymentList{}
	err := r.client.List(context.TODO(), matchingClusterDeployments, &client.ListOptions{})
	return matchingClusterDeployments, err
}

func Find(slice []hivev1.ClusterDeployment, val hivev1.ClusterDeployment) (int, bool) {
	for i, item := range slice {
		if item.Namespace == val.Namespace && item.Name == val.Name {
			return i, true
		}
	}
	return -1, false
}

// Add finalizers to both the deadmanssnitch integreation and the matching cluster deployment
func (r *ReconcileDeadmansSnitchIntegration) dmsAddFinalizer(dmsi *deadmanssnitchv1alpha1.DeadmansSnitchIntegration, clusterdeployment *hivev1.ClusterDeployment) error {
	deadMansSnitchFinalizer := finalizerName(dmsi)
	logger := log.WithValues("DeadMansSnitchIntegreation.Namespace", dmsi.Namespace, "DeadMansSnitchIntegreation.Name", dmsi.Name, "ClusterDeployment.Name:", clusterdeployment.Name, "ClusterDeployment.Namespace:", clusterdeployment.Namespace)
	//checking i finalizers exits in the clusterdeployment adding if they dont
	logger.Info("Checking for finalizers")
	if !utils.HasFinalizer(clusterdeployment, deadMansSnitchFinalizer) {
		log.Info(fmt.Sprint("Adding finalizer to cluster Deployment Name:  ", clusterdeployment.Name+" namespace:"+clusterdeployment.Namespace+" DMSI Name  :"+dmsi.Name))
		utils.AddFinalizer(clusterdeployment, deadMansSnitchFinalizer)
		err := r.client.Update(context.TODO(), clusterdeployment)
		if err != nil {
			return err
		}

	}
	logger.Info("Cluster deployment finalizer created nothing to do here ...: ")

	//checking i finalizers exits in the dmsi cr adding if they dont
	logger.Info("Checking for finalizers")
	if !utils.HasFinalizer(dmsi, deadMansSnitchFinalizer) {
		log.Info(fmt.Sprint("Adding finalizer to DMSI Name: ", " DMSI Name: :"+dmsi.Name))
		utils.AddFinalizer(dmsi, deadMansSnitchFinalizer)
		err := r.client.Update(context.TODO(), dmsi)
		if err != nil {
			return err
		}

	}
	logger.Info("DMSI finalizer created nothing to do here..: ")

	return nil

}

// create snitch in deadmanssnitch.com with information retrived from dmsi cr as well as the matching cluster deployment
func (r *ReconcileDeadmansSnitchIntegration) createSnitch(dmsi *deadmanssnitchv1alpha1.DeadmansSnitchIntegration, cd *hivev1.ClusterDeployment, dmsc dmsclient.Client) error {
	logger := log.WithValues("DeadMansSnitchIntegreation.Namespace", dmsi.Namespace, "DeadMansSnitchIntegreation.Name", dmsi.Name, "ClusterDeployment.Name:", cd.Name, "ClusterDeployment.Namespace:", cd.Namespace)
	snitchName := utils.DmsSnitchName(cd.Spec.ClusterName, cd.Spec.BaseDomain, dmsi.Spec.SnitchNamePostFix)

	ssName := utils.SecretName(cd.Spec.ClusterName, dmsi.Spec.SnitchNamePostFix)
	err := r.client.Get(context.TODO(), types.NamespacedName{Name: ssName, Namespace: cd.Namespace}, &hivev1.SyncSet{})

	if errors.IsNotFound(err) {
		logger.Info("Checking if snitch already exists SnitchName:", snitchName)
		snitches, err := dmsc.FindSnitchesByName(snitchName)
		if err != nil {
			return err
		}

		var snitch dmsclient.Snitch
		if len(snitches) > 0 {
			snitch = snitches[0]
		} else {
			newSnitch := dmsclient.NewSnitch(snitchName, dmsi.Spec.Tags, "15_minute", "basic")
			logger.Info("Creating snitch", snitchName)
			snitch, err = dmsc.Create(newSnitch)
			if err != nil {
				return err
			}
		}

		ReSnitches, err := dmsc.FindSnitchesByName(snitchName)
		if err != nil {
			return err
		}

		if len(ReSnitches) > 0 {
			if ReSnitches[0].Status == "pending" {
				logger.Info("Checking in Snitch ...")
				// CheckIn snitch
				err = dmsc.CheckIn(snitch)
				if err != nil {
					logger.Error(err, "Unable to check in deadman's snitch", "CheckInURL", snitch.CheckInURL)
					return err
				}
			}
		} else {
			logger.Error(err, "Unable to get Snitch by name")
			return err
		}
	}

	logger.Info("Snitch created nothing to do here.... ")
	return nil
}

//Create secret containing the snitch url
func (r *ReconcileDeadmansSnitchIntegration) createSecret(dmsi *deadmanssnitchv1alpha1.DeadmansSnitchIntegration, dmsc dmsclient.Client, cd hivev1.ClusterDeployment) error {
	logger := log.WithValues("DeadMansSnitchIntegreation.Namespace", dmsi.Namespace, "DeadMansSnitchIntegreation.Name", dmsi.Name, "ClusterDeployment.Name:", cd.Name, "ClusterDeployment.Namespace:", cd.Namespace)
	dmsSecret := utils.SecretName(cd.Spec.ClusterName, dmsi.Spec.SnitchNamePostFix)
	logger.Info("Checking if secret already exits")
	err := r.client.Get(context.TODO(),
		types.NamespacedName{Name: dmsSecret, Namespace: cd.Namespace},
		&corev1.Secret{})
	if errors.IsNotFound(err) {
		logger.Info("Secret not found creating secret")
		snitchName := utils.DmsSnitchName(cd.Spec.ClusterName, cd.Spec.BaseDomain, dmsi.Spec.SnitchNamePostFix)
		ReSnitches, err := dmsc.FindSnitchesByName(snitchName)

		if err != nil {
			return err
		}
		for _, CheckInURL := range ReSnitches {

			newdmsSecret := newDMSSecret(cd.Namespace, dmsSecret, CheckInURL.CheckInURL)

			// set the owner reference about the secret for gabage collection
			if err := controllerutil.SetControllerReference(&cd, newdmsSecret, r.scheme); err != nil {
				logger.Error(err, "Error setting controller reference on secret")
				return err
			}
			// Create the secret
			if err := r.client.Create(context.TODO(), newdmsSecret); err != nil {
				logger.Error(err, "Failed to create secret")
				return err
			}

		}
	}
	logger.Info("Secret created , nothing to do here...")
	return nil
}

//creating the syncset which contain the secret with the snitch url
func (r *ReconcileDeadmansSnitchIntegration) createSyncset(dmsi *deadmanssnitchv1alpha1.DeadmansSnitchIntegration, cd hivev1.ClusterDeployment) error {
	logger := log.WithValues("DeadMansSnitchIntegreation.Namespace", dmsi.Namespace, "DeadMansSnitchIntegreation.Name", dmsi.Name, "ClusterDeployment.Name:", cd.Name, "ClusterDeployment.Namespace:", cd.Namespace)
	ssName := utils.SecretName(cd.Spec.ClusterName, dmsi.Spec.SnitchNamePostFix)
	err := r.client.Get(context.TODO(), types.NamespacedName{Name: ssName, Namespace: cd.Namespace}, &hivev1.SyncSet{})

	if errors.IsNotFound(err) {
		logger.Info("SyncSet not found, Creating a new SyncSet")

		newSS := newSyncSet(cd.Namespace, ssName, cd.Name, dmsi)
		if err := controllerutil.SetControllerReference(&cd, newSS, r.scheme); err != nil {
			logger.Error(err, "Error setting controller reference on syncset")
			return err
		}
		if err := r.client.Create(context.TODO(), newSS); err != nil {
			logger.Error(err, "Error creating syncset")
			return err
		}
		logger.Info("Done creating a new SyncSet")

	} else {
		logger.Info("SyncSet Already Present, nothing to do here...")
		// return directly if the syscset already existed

	}

	return nil
}

func newDMSSecret(namespace string, name string, snitchURL string) *corev1.Secret {

	dmsSecret := &corev1.Secret{
		Type: "Opaque",
		TypeMeta: metav1.TypeMeta{
			Kind:       "Secret",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Data: map[string][]byte{
			config.KeySnitchURL: []byte(snitchURL),
		},
	}

	return dmsSecret
}

func newSyncSet(namespace string, dmsSecret string, clusterDeploymentName string, dmsi *deadmanssnitchv1alpha1.DeadmansSnitchIntegration) *hivev1.SyncSet {

	newSS := &hivev1.SyncSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dmsSecret,
			Namespace: namespace,
		},
		Spec: hivev1.SyncSetSpec{
			ClusterDeploymentRefs: []corev1.LocalObjectReference{
				{
					Name: clusterDeploymentName,
				},
			},
			SyncSetCommonSpec: hivev1.SyncSetCommonSpec{
				ResourceApplyMode: hivev1.SyncResourceApplyMode,
				Secrets: []hivev1.SecretMapping{
					{
						SourceRef: hivev1.SecretReference{
							Name:      dmsSecret,
							Namespace: namespace,
						},
						TargetRef: hivev1.SecretReference{
							Name:      dmsi.Spec.TargetSecretRef.Name,
							Namespace: dmsi.Spec.TargetSecretRef.Namespace,
						},
					},
				},
			},
		},
	}

	return newSS
}

func finalizerName(dmsi *deadmanssnitchv1alpha1.DeadmansSnitchIntegration) string {
	return config.FinalizerBase + dmsi.Name
}

// delete snitches,secrets and syncset associated with the cluster deployment that has been deleted
func (r *ReconcileDeadmansSnitchIntegration) deleteDMSClusterDeployment(dmsi *deadmanssnitchv1alpha1.DeadmansSnitchIntegration, clusterDeployment *hivev1.ClusterDeployment, dmsc dmsclient.Client) error {
	deadMansSnitchFinalizer := finalizerName(dmsi)
	logger := log.WithValues("DeadMansSnitchIntegreation.Namespace", dmsi.Namespace, "DeadMansSnitchIntegreation.Name", dmsi.Name, "ClusterDeployment.Name:", clusterDeployment.Name, "ClusterDeployment.Namespace:", clusterDeployment.Namespace)
	// Delete the dms
	logger.Info("Deleting the DMS from api.deadmanssnitch.com")
	snitchName := utils.DmsSnitchName(clusterDeployment.Spec.ClusterName, clusterDeployment.Spec.BaseDomain, dmsi.Spec.SnitchNamePostFix)
	snitches, err := dmsc.FindSnitchesByName(snitchName)
	if err != nil {
		return err
	}
	for _, s := range snitches {
		delStatus, err := dmsc.Delete(s.Token)
		if !delStatus || err != nil {
			logger.Error(err, "Failed to delete the DMS from api.deadmanssnitch.com")
			return err
		}
		logger.Info("Deleted the DMS from api.deadmanssnitch.com")
	}

	// Delete the SyncSet
	logger.Info("Deleting DMS SyncSet")
	dmsSecret := utils.SecretName(clusterDeployment.Spec.ClusterName, dmsi.Spec.SnitchNamePostFix)
	err = utils.DeleteSyncSet(dmsSecret, clusterDeployment.Namespace, r.client)
	if err != nil {
		logger.Error(err, "Error deleting SyncSet")
		return err
	}

	// Delete the referenced secret
	logger.Info("Deleting DMS referenced secret")
	err = utils.DeleteRefSecret(dmsSecret, clusterDeployment.Namespace, r.client)
	if err != nil {
		logger.Error(err, "Error deleting secret")
		return err
	}

	if utils.HasFinalizer(clusterDeployment, deadMansSnitchFinalizer) {
		logger.Info("Deleting DMSI finalizer from cluster deployment")
		utils.DeleteFinalizer(clusterDeployment, deadMansSnitchFinalizer)
		err = r.client.Update(context.TODO(), clusterDeployment)
		if err != nil {
			logger.Error(err, "Error deleting Finalizer from cluster deployment")
			return err
		}
	}

	return nil
}
