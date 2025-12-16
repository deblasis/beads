package sqlite

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/types"
)

// getInode returns the inode of a file (Unix only)
func getInode(path string) uint64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	if sys := info.Sys(); sys != nil {
		if stat, ok := sys.(*syscall.Stat_t); ok {
			return stat.Ino
		}
	}
	return 0
}

// TestBranchMergeNoErroneousDeletion tests the full branch merge scenario.
// This is an end-to-end test for the daemon stale cache fix.
//
// Scenario:
// 1. Main has issue A in DB
// 2. Branch is created, issue B is added
// 3. Branch merged to main (DB file replaced)
// 4. WITHOUT fix: daemon's stale connection sees old DB (only A)
// 5. WITHOUT fix: if auto-import runs with NoGitHistory=false, B could be deleted
// 6. WITH fix: freshness checker detects file replacement, reconnects, sees A and B
//
// On main (without fix): daemon doesn't see issue B after merge
// On fix branch: daemon sees both issues correctly
func TestBranchMergeNoErroneousDeletion(t *testing.T) {
	// === SETUP: Create "main" database with issue A ===
	tmpDir1, err := os.MkdirTemp("", "beads-merge-test-main-*")
	if err != nil {
		t.Fatalf("failed to create main temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir1)

	mainDBPath := filepath.Join(tmpDir1, "beads.db")
	ctx := context.Background()

	// Create main store with issue A
	mainStore, err := New(ctx, mainDBPath)
	if err != nil {
		t.Fatalf("failed to create main store: %v", err)
	}
	mainStore.SetConfig(ctx, "issue_prefix", "bd")

	issueA := &types.Issue{
		Title:     "Issue A (existed on main)",
		Status:    types.StatusOpen,
		Priority:  2,
		IssueType: types.TypeTask,
	}
	if err := mainStore.CreateIssue(ctx, issueA, "test-user"); err != nil {
		t.Fatalf("failed to create issue A: %v", err)
	}
	issueAID := issueA.ID
	t.Logf("Created issue A on main: %s", issueAID)
	mainStore.Close()

	// === SETUP: Create "branch" database with issues A and B ===
	tmpDir2, err := os.MkdirTemp("", "beads-merge-test-branch-*")
	if err != nil {
		t.Fatalf("failed to create branch temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir2)

	branchDBPath := filepath.Join(tmpDir2, "beads.db")

	// Create branch store and copy issue A, then add issue B
	branchStore, err := New(ctx, branchDBPath)
	if err != nil {
		t.Fatalf("failed to create branch store: %v", err)
	}
	branchStore.SetConfig(ctx, "issue_prefix", "bd")

	// Copy issue A to branch
	issueACopy := &types.Issue{
		ID:        issueAID,
		Title:     "Issue A (existed on main)",
		Status:    types.StatusOpen,
		Priority:  2,
		IssueType: types.TypeTask,
	}
	if err := branchStore.CreateIssue(ctx, issueACopy, "test-user"); err != nil {
		t.Fatalf("failed to copy issue A to branch: %v", err)
	}

	// Create issue B on branch
	issueB := &types.Issue{
		Title:     "Issue B (created on branch)",
		Status:    types.StatusOpen,
		Priority:  1,
		IssueType: types.TypeFeature,
	}
	if err := branchStore.CreateIssue(ctx, issueB, "test-user"); err != nil {
		t.Fatalf("failed to create issue B: %v", err)
	}
	issueBID := issueB.ID
	t.Logf("Created issue B on branch: %s", issueBID)
	branchStore.Close()

	// === SIMULATE DAEMON: Open main DB ===
	daemonStore, err := New(ctx, mainDBPath)
	if err != nil {
		t.Fatalf("failed to create daemon store: %v", err)
	}
	defer daemonStore.Close()

	// NOTE: On the fix branch, EnableFreshnessChecking() is called here.
	// Without the fix, this method doesn't exist and the daemon will have stale data.
	daemonStore.SetConfig(ctx, "issue_prefix", "bd")

	// Verify daemon sees only issue A initially
	issuesBeforeMerge, _ := daemonStore.SearchIssues(ctx, "", types.IssueFilter{})
	t.Logf("Daemon sees %d issue(s) before merge", len(issuesBeforeMerge))
	if len(issuesBeforeMerge) != 1 {
		t.Errorf("Expected 1 issue before merge, got %d", len(issuesBeforeMerge))
	}

	// === SIMULATE GIT MERGE: Replace main DB file with branch DB ===
	inodeBefore := getInode(mainDBPath)

	// Remove WAL/SHM files
	os.Remove(mainDBPath + "-wal")
	os.Remove(mainDBPath + "-shm")

	// Read branch DB and atomically replace main DB
	branchDBContent, err := os.ReadFile(branchDBPath)
	if err != nil {
		t.Fatalf("failed to read branch DB: %v", err)
	}

	tempFile := mainDBPath + ".new"
	if err := os.WriteFile(tempFile, branchDBContent, 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	if err := os.Rename(tempFile, mainDBPath); err != nil {
		t.Fatalf("failed to rename: %v", err)
	}

	inodeAfter := getInode(mainDBPath)
	t.Logf("Merge simulation: inode %d -> %d", inodeBefore, inodeAfter)

	// Small delay to ensure filesystem settles
	time.Sleep(100 * time.Millisecond)

	// === VERIFY: Daemon should see BOTH issues after merge ===
	// The freshness checker should detect the file replacement and reconnect

	issueAResult, err := daemonStore.GetIssue(ctx, issueAID)
	if err != nil || issueAResult == nil {
		t.Errorf("Issue A not visible after merge: err=%v", err)
	} else {
		t.Logf("Issue A visible after merge: %s", issueAResult.Title)
	}

	issueBResult, err := daemonStore.GetIssue(ctx, issueBID)
	if err != nil || issueBResult == nil {
		t.Errorf("Issue B not visible after merge (this is the bug!): err=%v", err)
	} else {
		t.Logf("Issue B visible after merge: %s", issueBResult.Title)
	}

	issuesAfterMerge, _ := daemonStore.SearchIssues(ctx, "", types.IssueFilter{})
	t.Logf("Daemon sees %d issue(s) after merge", len(issuesAfterMerge))

	if len(issuesAfterMerge) != 2 {
		t.Errorf("Expected 2 issues after merge, got %d", len(issuesAfterMerge))
		t.Logf("This demonstrates the stale cache bug - daemon doesn't see merged changes")
	}

	// === VERIFY: No erroneous deletions ===
	// In a buggy scenario without NoGitHistory protection, issue B could be
	// incorrectly added to deletions.jsonl. With the freshness fix, the daemon
	// sees the correct DB state and no deletion occurs.

	// Check if any deletions occurred (they shouldn't)
	// Note: This test doesn't create a deletions.jsonl file, so we verify
	// by ensuring both issues are still accessible
	finalIssueA, _ := daemonStore.GetIssue(ctx, issueAID)
	finalIssueB, _ := daemonStore.GetIssue(ctx, issueBID)

	if finalIssueA == nil {
		t.Error("ERRONEOUS DELETION: Issue A was deleted!")
	}
	if finalIssueB == nil {
		t.Error("ERRONEOUS DELETION: Issue B was deleted!")
	}
}
