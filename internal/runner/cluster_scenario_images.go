package runner

// Moving the nodes of a scenario case to another image: an upgrade from an
// earlier release, or the way back to it.
//
// A case starts its nodes on cluster.scenario.start_image and keeps one
// directory of every node on a Docker volume of the node's own
// (cluster.scenario.node_volume). A start step with `image` gives a stopped
// node a new container from that image, and a rolling_restart step with
// `image` does the same to every node in turn. The new container finds the
// node's data in the volume, as a director that an upgrade replaced on its
// machine finds its data on the machine's disk.
//
// Docker drops a container's log with the container. The driver keeps the
// log of every container it replaces, and the log checks read the kept logs
// ahead of the new container's own, so a check that spans the replacement
// still sees what the old version logged.

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/VirtualMetric/PipeBench/internal/config"
)

// scenarioImage is one image a node ran, from the step that started it on
// that image. Step is -1 for the image the node started the run on.
type scenarioImage struct {
	Node    int               `json:"node"`
	Step    int               `json:"step"`
	Image   string            `json:"image"`
	ImageID string            `json:"image_id,omitempty"`
	Labels  map[string]string `json:"image_labels,omitempty"`
	At      time.Time         `json:"at"`
}

// keptLog is the log of a node container that a step replaced, as
// `docker logs -t` printed it just before the replacement.
type keptLog struct {
	Step  int
	Image string
	Text  string
}

// containerLogStore holds the kept logs of replaced containers, by container
// name, oldest first.
type containerLogStore struct {
	mu          sync.Mutex
	byContainer map[string][]keptLog
}

// replacedLogs is the store readLogWindow reads. A run resets it at its start
// and its end: container names repeat from run to run.
var replacedLogs = &containerLogStore{byContainer: map[string][]keptLog{}}

func (s *containerLogStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byContainer = map[string][]keptLog{}
}

func (s *containerLogStore) keep(container string, l keptLog) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byContainer[container] = append(s.byContainer[container], l)
}

func (s *containerLogStore) all(container string) []keptLog {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]keptLog(nil), s.byContainer[container]...)
}

// window returns the lines of the container's kept logs stamped from since up
// to until, oldest first. A zero until leaves the window open.
func (s *containerLogStore) window(container string, since, until time.Time) string {
	var b strings.Builder
	for _, l := range s.all(container) {
		b.WriteString(logLinesInWindow(l.Text, since, until))
	}
	return b.String()
}

// logLinesInWindow returns the lines of `docker logs -t` output stamped from
// since up to until. A line whose stamp cannot be read is left out.
func logLinesInWindow(text string, since, until time.Time) string {
	var b strings.Builder
	for _, line := range strings.Split(text, "\n") {
		ts, _, _ := strings.Cut(line, " ")
		at, err := time.Parse(time.RFC3339Nano, ts)
		if err != nil || at.Before(since) || (!until.IsZero() && at.After(until)) {
			continue
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return b.String()
}

// imageRef turns a step's image into an image reference: "subject" is the
// image under test.
func (sr *scenarioRun) imageRef(image string) string {
	if image == config.SubjectImageKeyword {
		return sr.subject.ImageRef()
	}
	return image
}

// startImageRef is the image every node starts the run on.
func (sr *scenarioRun) startImageRef() string {
	if sr.sc.StartImage != "" {
		return sr.sc.StartImage
	}
	return sr.subject.ImageRef()
}

// imageFullID returns the full ID of a local image, pulling the image first
// when the machine does not have it. It returns "" when the image cannot be
// found or pulled.
func imageFullID(ctx context.Context, ref string) string {
	inspect := func() string {
		out, err := exec.Command("docker", "image", "inspect", "-f", "{{.Id}}", ref).Output()
		if err != nil {
			return ""
		}
		return strings.TrimSpace(string(out))
	}
	if id := inspect(); id != "" {
		return id
	}
	pctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	if err := exec.CommandContext(pctx, "docker", "pull", "-q", ref).Run(); err != nil {
		return ""
	}
	return inspect()
}

// containerImageID returns the full ID of the image a container was created
// from.
func containerImageID(container string) string {
	out, err := exec.Command("docker", "inspect", "-f", "{{.Image}}", container).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func shortImageID(id string) string {
	id = strings.TrimPrefix(id, "sha256:")
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

// noteStartImages records the image every node starts the run on.
func (sr *scenarioRun) noteStartImages() {
	ref := sr.startImageRef()
	id, labels := imageIdentity(ref)
	for n := 1; n <= len(sr.containers); n++ {
		sr.nodeImage[n] = ref
		if !sr.active[n] {
			continue
		}
		sr.images = append(sr.images, scenarioImage{Node: n, Step: -1, Image: ref, ImageID: id, Labels: labels, At: sr.upAt})
	}
	if sr.sc.StartImage != "" {
		fmt.Printf("  the nodes start on %s (%s %v)\n", ref, id, labels)
	}
}

// nodeImagesForReport returns the images the nodes ran, for a case that
// moves nodes between images. Other cases leave the list out: their nodes all
// ran the image under test, which the report names already.
func (sr *scenarioRun) nodeImagesForReport() []scenarioImage {
	if sr.sc.StartImage != "" {
		return sr.images
	}
	for _, im := range sr.images {
		if im.Step >= 0 {
			return sr.images
		}
	}
	return nil
}

// moveNodeToImage gives node n a new container from image, on the data its
// volume keeps. The node must not be running: the step before it says how
// the old version stopped (a clean stop or a kill). It keeps the old
// container's log, and checks that the new container runs the image. It
// returns why it failed, or "" and a line for the report.
func (sr *scenarioRun) moveNodeToImage(step, n int, image string) (detail, failure string) {
	ref := sr.imageRef(image)
	c := sr.containers[n-1]
	running, err := containerRunningState(c)
	if err != nil {
		return "", fmt.Sprintf("could not tell whether node %d runs: %v", n, err)
	}
	if running {
		return "", fmt.Sprintf("node %d still runs; a step must stop or kill it before a start with image, so that the case says how the old version stopped", n)
	}
	wantID := imageFullID(sr.ctx, ref)
	if wantID == "" {
		return "", fmt.Sprintf("image %s is not on this machine and could not be pulled", ref)
	}
	// Docker drops the old container's log with the container. Without the
	// log, the evidence of what the old version did is gone, so the step
	// fails rather than go on without it.
	out, err := exec.Command("docker", "logs", "-t", c).CombinedOutput()
	if err != nil {
		return "", fmt.Sprintf("could not keep node %d's log before replacing its container: %v: %s", n, err, strings.TrimSpace(string(out)))
	}
	replacedLogs.keep(c, keptLog{Step: step, Image: sr.nodeImage[n], Text: string(out)})
	if err := sr.orch.RecreateServiceWithImage(fmt.Sprintf("subject-%d", n), ref); err != nil {
		return "", fmt.Sprintf("starting node %d on %s: %v", n, ref, err)
	}
	if got := containerImageID(c); got != wantID {
		return "", fmt.Sprintf("node %d's new container runs image %s, not %s (%s)", n, shortImageID(got), ref, shortImageID(wantID))
	}
	from := sr.nodeImage[n]
	sr.nodeImage[n] = ref
	_, labels := imageIdentity(ref)
	sr.images = append(sr.images, scenarioImage{Node: n, Step: step, Image: ref, ImageID: shortImageID(wantID), Labels: labels, At: time.Now()})
	return fmt.Sprintf("started node %d on %s (%s), on the data %s left in its volume", n, ref, shortImageID(wantID), from), ""
}
