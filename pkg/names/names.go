package names

const MANAGER_NAMESPACE = "kubemacpool-system"

const MANAGER_DEPLOYMENT = "kubemacpool-mac-controller-manager"

const WEBHOOK_SERVICE = "kubemacpool-service"

const MUTATE_WEBHOOK = "kubemacpool-webhook"

const MUTATE_WEBHOOK_CONFIG = "kubemacpool"

const LEADER_LABEL = "kubemacpool-leader"

const LEADER_ID = "kubemacpool-election"

const K8S_RUNLABEL = "runlevel"

const OPENSHIFT_RUNLABEL = "openshift.io/run-level"

var CRITICAL_RUNLABELS = []string{"0", "1"}
