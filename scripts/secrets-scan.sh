#!/usr/bin/env bash
#
# §50 · Secret verification.
#
# Scans git-TRACKED content only. Untracked files are irrelevant to leakage and
# scanning them produces noise (.env is supposed to contain secrets; the point is
# that it never gets tracked).
#
# Run standalone, or as a pre-commit hook:
#   ln -s ../../scripts/secrets-scan.sh .git/hooks/pre-commit
#
#   ./scripts/secrets-scan.sh            # scan the committed tree
#   ./scripts/secrets-scan.sh --staged   # scan what is about to be committed
#
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1
MODE="${1:-}"
FINDINGS=0

report() {
    printf '  \033[31mFINDING\033[0m  %s\n' "$1"
    [ -n "${2:-}" ] && printf '            %s\n' "$2"
    FINDINGS=$((FINDINGS + 1))
}

ok() { printf '  \033[32mok\033[0m       %s\n' "$1"; }

if [ "$MODE" = "--staged" ]; then
    FILES=$(git diff --cached --name-only --diff-filter=ACM)
else
    FILES=$(git ls-files)
fi

echo "== files that must never be tracked =="

for forbidden in .env .env.local .env.production id_rsa id_ed25519 .netrc credentials.json; do
    if git ls-files --error-unmatch "$forbidden" >/dev/null 2>&1; then
        report "$forbidden is tracked by git" "remove with: git rm --cached $forbidden"
    fi
done
git ls-files | grep -E '(^|/)\.env($|\.)' | grep -v '\.env\.example$' | while read -r f; do
    report "$f is tracked by git"
done
[ "$FINDINGS" -eq 0 ] && ok "no credential files tracked"

echo
echo "== .env is ignored =="
if git check-ignore -q .env 2>/dev/null; then
    ok ".env is covered by .gitignore"
else
    report ".env is NOT gitignored" "add '.env' to .gitignore"
fi

echo
echo "== .env.example carries placeholders only =="
if [ -f .env.example ]; then
    # A value that is long and random-looking in the template means someone
    # pasted a real secret into it.
    if grep -nE '^[A-Z_]+=[A-Za-z0-9+/]{16,}$' .env.example \
        | grep -v 'CHANGE_ME' >/dev/null 2>&1; then
        report ".env.example contains what looks like a real value" \
               "$(grep -nE '^[A-Z_]+=[A-Za-z0-9+/]{16,}$' .env.example | grep -v CHANGE_ME | head -3)"
    else
        ok ".env.example holds placeholders only"
    fi
fi

echo
echo "== hardcoded credentials in tracked files =="

# Assignments of a literal secret. Deliberately narrow: matching every mention
# of the word "password" would flag documentation and column names, and a
# scanner people learn to ignore protects nothing.
PATTERNS=(
  '(PASSWORD|PASSWD|SECRET|API_?KEY|ACCESS_?TOKEN|PRIVATE_?KEY)[[:space:]]*[:=][[:space:]]*["'"'"']?[A-Za-z0-9_./+-]{3,}'
  '(postgres|postgresql|mysql|clickhouse|mongodb|redis)://[^:/@[:space:]]+:[^@[:space:]]+@'
  'BEGIN (RSA|OPENSSH|DSA|EC|PGP) PRIVATE KEY'
  'AKIA[0-9A-Z]{16}'
  'ghp_[A-Za-z0-9]{36}'
  'sk-[A-Za-z0-9]{32,}'
)

for file in $FILES; do
    [ -f "$file" ] || continue
    case "$file" in
        .env.example|scripts/secrets-scan.sh|docs/*) continue ;;   # template, this scanner, prose
    esac
    for pattern in "${PATTERNS[@]}"; do
        while IFS= read -r hit; do
            [ -z "$hit" ] && continue
            # Compose/shell interpolation (${VAR} or $VAR) is a reference, not a
            # literal, and is exactly the pattern we want people using.
            value="${hit#*:}"
            case "$hit" in
                *'${'*|*'$('*) continue ;;
            esac
            # Env-var passthrough like PGPASSWORD="$POSTGRES_PASSWORD".
            case "$hit" in
                *'="$'*|*"='\$"*|*'=$'*) continue ;;
            esac
            report "$file: $(echo "$hit" | cut -c1-110)"
        # -i because `password = "..."` leaks just as much as `PASSWORD=...`;
        # a case-sensitive scan misses the lowercase form entirely.
        done < <(grep -nEIi "$pattern" "$file" 2>/dev/null || true)
    done
done

echo
echo "== credentials must not be logged =="
# A log/print statement interpolating a password variable.
while IFS= read -r hit; do
    [ -z "$hit" ] && continue
    report "possible credential logging: $(echo "$hit" | cut -c1-110)"
done < <(git grep -nEI '(echo|print|println|log\.(info|debug|warn|error)|System\.out)[^;]*(PASSWORD|passwd|SECRET)' \
         -- '*.java' '*.sh' 2>/dev/null | grep -vE 'secrets-scan|not echoed|CHANGE_ME|is not set|still holds' || true)
[ "$FINDINGS" -eq 0 ] && ok "no credential logging found"

echo
if [ "$FINDINGS" -eq 0 ]; then
    printf '\n  \033[32mclean\033[0m — no secrets found in tracked content\n\n'
    exit 0
fi
printf '\n  \033[31m%d finding(s)\033[0m\n\n' "$FINDINGS"
exit 1
