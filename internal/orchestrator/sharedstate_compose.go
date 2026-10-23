package orchestrator

// applySharedStateTopology fills the compose variables a
// director_shared_state_correctness case needs beyond its replicas. Other
// cases are left as they were.
func applySharedStateTopology(vars *composeVars, cfg RunConfig) error {
	if !cfg.TestCase.IsDirectorSharedStateType() {
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
	return nil
}
