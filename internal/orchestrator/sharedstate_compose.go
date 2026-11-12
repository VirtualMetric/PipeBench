package orchestrator

import "github.com/VirtualMetric/PipeBench/internal/config"

// A share on a server needs a known address: Docker mounts it in the host's
// kernel, which cannot resolve a compose service's name. The bench network
// then takes a fixed subnet, the one the cluster scenarios pin as well, and
// the server an address high in it, away from the ones Docker gives out.
const (
	shareBenchSubnet = "172.30.0.0/16"
	shareServerIP    = "172.30.250.250"
)

// applySharedStateTopology fills the compose variables a
// director_shared_state_correctness case needs beyond its replicas. Other
// cases are left as they were.
func applySharedStateTopology(vars *composeVars, cfg RunConfig) error {
	tc := cfg.TestCase
	if !tc.IsDirectorSharedStateType() {
		return nil
	}
	// The driver counts loss and copies on the numbers the generators put in
	// every line (/seqcheck). A line that arrives cut or garbled then carries
	// no number and is never counted as a record, where a count of distinct
	// lines would count it as one and hide a record that was lost.
	trackSeq := map[string]string{"RECEIVER_TRACK_SEQ": "true"}
	vars.RecvEnv = mergeEnv(vars.RecvEnv, trackSeq)
	for i := range vars.Receivers {
		vars.Receivers[i].Env = mergeEnv(vars.Receivers[i].Env, trackSeq)
	}

	// A share is a directory every replica mounts. It is the volume that the
	// cluster scenarios use for their witness directory: on the host's disk,
	// with the init that makes it writable for the director's user, or on an
	// SMB or an NFS server, which the replicas wait for.
	if ss := tc.SharedState; ss.StoreOrDefault() == config.SharedStateStoreShare {
		vars.WitnessDirMount = ss.SharePath
		if ss.ShareInVolume() {
			vars.NodeDependsOn = append(vars.NodeDependsOn, nodeDependency{"witness-init", "service_completed_successfully"})
		} else {
			if vars.BenchSubnet == "" {
				vars.BenchSubnet = shareBenchSubnet
			}
			vars.ShareServer, vars.ShareServerImage, vars.ShareServerIP = ss.ShareServer, ss.ShareServerImage(), shareServerIP
			vars.NodeDependsOn = append(vars.NodeDependsOn, nodeDependency{config.ShareServerService, "service_healthy"})
		}
	}
	// The replicas wait for the bucket an S3 store needs, as they wait for
	// Azurite's containers.
	if tc.UsesMinio() {
		vars.NodeDependsOn = append(vars.NodeDependsOn, nodeDependency{"minio-init", "service_completed_successfully"})
	}
	return nil
}
