#!/usr/bin/env bash
# Set up a git worktree of pfeed created by the Zed editor.
#
# Zed creates git worktrees at:
#
#   pfund.ai/worktrees/pfeed/<branch>/pfeed
#
# instead of the main checkout at:
#
#   pfund.ai/pfeed
#
# Problem 1: Zed leaves the worktree on a detached HEAD without a branch.
# Solution: create a branch named after the worktree directory (<branch> above).
#
# Problem 2: if pixi.toml references sibling repos by relative path (e.g. "../pfund"),
# from the worktree location those paths point to nothing,
# so pixi fails to resolve the environment.
# Solution: symlink the sibling repos (SIBLINGS below) into the worktree's parent
# directory, so "../pfund" etc. resolve to the real repos next to the main checkout.
# pfeed's pixi.toml has none for now, so SIBLINGS is empty.
#
# Problem 3: git-ignored local files and folders (.env, and sources/ with the
# pfeed-* source repos) are not part of any commit, so a fresh worktree doesn't have them.
# Solution: symlink them from the main checkout into the worktree.
#
# Problem 4: a fresh worktree has no Pixi environment for the editor to find.
# Solution: run pixi install when pixi.toml exists to create .pixi locally.
# --frozen installs exactly what pixi.lock says without updating it: otherwise pixi
# resolves the sibling symlinks to their real paths ("../pfund" -> "../../../../pfund")
# and rewrites pixi.lock, which shows up as a change in every new worktree.
#
# Usage: scripts/worktree-symlinks.sh <path-to-worktree-checkout>

set -e

# the `git switch` below fires post-checkout again, which would re-run this script
# (and pixi install) inside itself, so the nested run does nothing
[ -n "$WORKTREE_SYMLINKS_RUNNING" ] && exit 0
export WORKTREE_SYMLINKS_RUNNING=1

# sibling repos to link next to the worktree (relative to pfund.ai/)
SIBLINGS=(
)

# git-ignored files to link from the main checkout (relative to pfeed/)
LOCAL_FILES=(
  .env
  sources
)

WORKTREE=$(cd "$1" && pwd)
COMMON=$(cd "$WORKTREE" && git rev-parse --path-format=absolute --git-common-dir)
MAIN_CHECKOUT=$(dirname "$COMMON")
MAIN=$(dirname "$MAIN_CHECKOUT")
PARENT=$(dirname "$WORKTREE")
# Zed maps a worktree name like "feature/x" to worktrees/pfeed/feature/x/pfeed,
# so the branch name is the parent path relative to worktrees/pfeed.
WORKTREES_ROOT="$MAIN/worktrees/$(basename "$MAIN_CHECKOUT")"
BRANCH="${PARENT#"$WORKTREES_ROOT"/}"

[ "$PARENT" = "$MAIN" ] && exit 0

# 1. branch (only when Zed left us on a detached HEAD)
if ! git -C "$WORKTREE" symbolic-ref -q HEAD >/dev/null; then
  if git -C "$WORKTREE" show-ref --verify --quiet "refs/heads/$BRANCH"; then
    git -C "$WORKTREE" switch -q "$BRANCH"
    echo "branch:  switched to existing '$BRANCH'"
  else
    git -C "$WORKTREE" switch -q -c "$BRANCH"
    echo "branch:  created '$BRANCH'"
  fi
fi

# 2. sibling repos
for repo in "${SIBLINGS[@]}"; do
  if [ -e "$MAIN/$repo" ]; then
    ln -sfn "$MAIN/$repo" "$PARENT/$repo"
    echo "sibling: $PARENT/$repo -> $MAIN/$repo"
  else
    echo "sibling: skipped $repo (not found at $MAIN/$repo)"
  fi
done

# 3. local files
for file in "${LOCAL_FILES[@]}"; do
  if [ -e "$MAIN_CHECKOUT/$file" ]; then
    ln -sfn "$MAIN_CHECKOUT/$file" "$WORKTREE/$file"
    echo "local:   $WORKTREE/$file -> $MAIN_CHECKOUT/$file"
  else
    echo "local:   skipped $file (not found at $MAIN_CHECKOUT/$file)"
  fi
done

# 4. Pixi environment (after linking dependencies and local files)
if [ -f "$WORKTREE/pixi.toml" ]; then
  echo "pixi:    installing environment in $WORKTREE"
  # falls back to a normal install when pixi.lock is out of date (e.g. a path dependency
  # it lists no longer exists), so the worktree still gets an environment
  if ! (cd "$WORKTREE" && pixi install --frozen); then
    echo "pixi:    pixi.lock is out of date, running pixi install (this updates pixi.lock)"
    (cd "$WORKTREE" && pixi install)
  fi
fi
