package orchestrator

import "github.com/VirtualMetric/PipeBench/internal/config"

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

	// A share is a directory every replica mounts. It is the volume, and the
	// init that makes it writable for the director's user, that the cluster
	// scenarios use for their witness directory.
	if ss := tc.SharedState; ss.StoreOrDefault() == config.SharedStateStoreShare {
		vars.WitnessDirMount = ss.SharePath
		vars.NodeDependsOn = append(vars.NodeDependsOn, nodeDependency{"witness-init", "service_completed_successfully"})
	}
	// The replicas wait for the bucket an S3 store needs, as they wait for
	// Azurite's containers.
	if tc.UsesMinio() {
		vars.NodeDependsOn = append(vars.NodeDependsOn, nodeDependency{"minio-init", "service_completed_successfully"})
	}
	return nil
}
