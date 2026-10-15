#!/bin/bash
set -e

# ==============================================================================
# Snowflake Git Diff Selective Deployment Script (Pattern 2)
# ==============================================================================
# Usage:
#   ./deploy_diff.sh [base_git_ref]
# Example:
#   ./deploy_diff.sh origin/main~1
# ==============================================================================

BASE_REF="${1:-HEAD~1}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=========================================================="
echo "Snowflake Git Diff Selective Deployment (Pattern 2)"
echo "Base Ref:    ${BASE_REF}"
echo "Project Dir: ${SCRIPT_DIR}"
echo "=========================================================="

# Git Base 유효성 확인
if git rev-parse "${BASE_REF}" >/dev/null 2>&1; then
  DIFF_TARGET="${BASE_REF}"
elif git rev-parse HEAD~1 >/dev/null 2>&1; then
  DIFF_TARGET="HEAD~1"
else
  DIFF_TARGET="4b825dc642cb6eb9a060e54bf8d69288fbee4904" # Empty tree hash
fi

# 변경되거나 추가된 .sql 파일 목록 추출 (Deleted 파일 제외)
CHANGED_SQL_FILES=$(git diff --name-only --diff-filter=ACMR "${DIFF_TARGET}" HEAD -- "${SCRIPT_DIR}/sql" 2>/dev/null || true)

if [ -z "$CHANGED_SQL_FILES" ]; then
  echo "No SQL changes detected in ${SCRIPT_DIR}/sql/"
  exit 0
fi

echo "Detected changed SQL files:"
echo "$CHANGED_SQL_FILES"
echo "----------------------------------------------------------"

# 1. Stored Procedures 배포 (01_procedures)
PROCS=$(echo "$CHANGED_SQL_FILES" | grep 'sql/01_procedures/' || true)
if [ -n "$PROCS" ]; then
  echo ">>> Deploying Changed Stored Procedures..."
  echo "$PROCS" | while read -r file; do
    if [ -f "$file" ]; then
      echo "  -> Executing: $file"
      snow sql -f "$file" -x
    fi
  done
else
  echo "No procedures changed."
fi

# 2. Tasks 배포 (02_tasks)
TASKS=$(echo "$CHANGED_SQL_FILES" | grep 'sql/02_tasks/' || true)
if [ -n "$TASKS" ]; then
  echo ">>> Deploying Changed Tasks..."
  echo "$TASKS" | while read -r file; do
    if [ -f "$file" ] && [[ "$file" != *"03_resume_tasks.sql"* ]]; then
      echo "  -> Executing: $file"
      snow sql -f "$file" -x
    fi
  done

  # 태스크 정의 변경 시 항상 역순 RESUME 스크립트 실행
  RESUME_SCRIPT="${SCRIPT_DIR}/sql/02_tasks/03_resume_tasks.sql"
  if [ -f "$RESUME_SCRIPT" ]; then
    echo ">>> Resuming Tasks DAG in reverse order..."
    snow sql -f "$RESUME_SCRIPT" -x
  fi
else
  echo "No tasks changed."
fi

echo "=========================================================="
echo "Selective Deployment Completed Successfully!"
echo "=========================================================="
