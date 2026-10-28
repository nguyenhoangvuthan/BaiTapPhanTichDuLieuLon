#!/usr/bin/env bash
# Dùng: source loi_giai/kich_hoat_moi_truong.sh
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    echo 'Hãy dùng: source loi_giai/kich_hoat_moi_truong.sh' >&2
    exit 1
fi

_labs_activate() {
    local labs_root labs_candidate labs_version labs_java_home=''
    labs_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)" || return 1
    if [[ ! -x "$labs_root/.venv-labs/bin/python" ]]; then
        echo "Chưa có môi trường Python: $labs_root/.venv-labs" >&2
        return 1
    fi

    # Bỏ qua JAVA_HOME sai hoặc Java ngoài các phiên bản Spark 4.0 hỗ trợ.
    for labs_candidate in "${JAVA_HOME:-}" "$labs_root/.work/java-21" /usr/lib/jvm/*; do
        [[ -x "$labs_candidate/bin/java" ]] || continue
        labs_version="$("$labs_candidate/bin/java" -version 2>&1)" || continue
        labs_version="$(sed -n 's/.*version "\([0-9]*\).*/\1/p' <<< "$labs_version")"
        if [[ "$labs_version" == 21 || "$labs_version" == 17 ]]; then
            labs_java_home="$labs_candidate"
            break
        fi
    done
    if [[ -z "$labs_java_home" ]]; then
        echo 'Chưa tìm thấy Java 17/21. Cài JDK 21 rồi source lại script.' >&2
        echo 'Ubuntu: sudo apt-get install openjdk-21-jdk-headless' >&2
        echo 'Hoặc giải nén JDK 21 vào .work/java-21/ (phải có bin/java).' >&2
        return 1
    fi

    source "$labs_root/.venv-labs/bin/activate" || return 1
    export JAVA_HOME="$labs_java_home"
    export PATH="$JAVA_HOME/bin:$PATH"
    export PYSPARK_PYTHON="$labs_root/.venv-labs/bin/python"
    export SPARK_LOCAL_IP=127.0.0.1
    printf 'JAVA_HOME=%s\nPYSPARK_PYTHON=%s\n' "$JAVA_HOME" "$PYSPARK_PYTHON"
}

if _labs_activate; then
    unset -f _labs_activate
else
    unset -f _labs_activate
    return 1
fi
