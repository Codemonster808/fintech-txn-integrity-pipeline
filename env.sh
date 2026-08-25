# env.sh — load AWS/LLM/vector vars and show what was set.
#
# Usage (from the repo root):
#   source env.sh           # verbose: prints every variable (learn mode)
#   source env.sh --quiet   # silent: for Makefiles
#
# Do NOT run this file (`bash env.sh`). It must be sourced so exports
# stick in your current shell.

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  echo "Don't execute this file. From the repo root run:" >&2
  echo "  source env.sh" >&2
  exit 1
fi

_ENV_QUIET=0
for _arg in "$@"; do
  [[ "$_arg" == "--quiet" ]] && _ENV_QUIET=1
done

_ENV_SH="${BASH_SOURCE[0]}"
_REPO_ROOT="$(cd "$(dirname "$_ENV_SH")" && pwd)"

_env_load() {
  local f="$1"
  if [[ -f "$f" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$f"
    set +a
    [[ $_ENV_QUIET -eq 1 ]] || echo "  loaded $f"
  fi
}

# Order: defaults → local overrides → secrets outside the repo.
_env_load "$_REPO_ROOT/.env.example"
_env_load "$_REPO_ROOT/.env"
_env_load "$HOME/.config/de-portfolio/.env"

# Belt-and-suspenders so a half-written .env still works.
export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://localhost:4566}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export LLM_PROVIDER="${LLM_PROVIDER:-fake}"
export VECTOR_BACKEND="${VECTOR_BACKEND:-chroma}"

# AWS CLI v2 reads AWS_ENDPOINT_URL natively (no --endpoint-url needed).
# awslocal is just a memory aid from the LocalStack world.
alias awslocal="aws --endpoint-url=$AWS_ENDPOINT_URL"

if [[ -f "$_REPO_ROOT/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "$_REPO_ROOT/.venv/bin/activate"
  [[ $_ENV_QUIET -eq 1 ]] || echo "  venv: $_REPO_ROOT/.venv"
fi

if [[ $_ENV_QUIET -eq 0 ]]; then
  echo "=== env.sh  ($_REPO_ROOT) ==="
  echo "  AWS_ENDPOINT_URL      = $AWS_ENDPOINT_URL"
  echo "  AWS_REGION            = $AWS_REGION"
  echo "  AWS_ACCESS_KEY_ID     = $AWS_ACCESS_KEY_ID"
  echo "  LLM_PROVIDER          = $LLM_PROVIDER"
  echo "  VECTOR_BACKEND        = $VECTOR_BACKEND"
  echo "  MINIMAX_API_KEY set?  = $([[ -n ${MINIMAX_API_KEY:-} ]] && echo yes || echo no)"
  echo "  PINECONE_API_KEY set? = $([[ -n ${PINECONE_API_KEY:-} ]] && echo yes || echo no)"
  echo
  echo "  Health check:  curl -s \$AWS_ENDPOINT_URL/health"
  echo "  Next:          docker compose up -d && python3 scripts/bootstrap.py"
  echo "  Inspect:       python3 scripts/aws_inspect.py all"
  echo "  AWS CLI:       aws s3 ls   (or awslocal s3 ls)"
  echo "=== (these exports live in THIS terminal only) ==="
fi

unset _ENV_QUIET _ENV_SH _arg
# keep _REPO_ROOT in case a recipe wants it
export REPO_ROOT="$_REPO_ROOT"
unset _REPO_ROOT
