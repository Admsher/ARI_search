#!/bin/bash

start_time=$(date +%s)
start_time_human=$(date)
script_name=$(readlink -f "$0")
base_folder="$1"
input_file="$2"
max_threads="$3"
affinity_info=$(taskset -p $$ 2>/dev/null)

echo "===== Script Started ====="
echo "Script: $script_name"
echo "Start Time: $start_time_human"
echo "Base Folder: $base_folder"
echo "Input File: $input_file"
echo "Max Threads: $max_threads"
echo "Affinity Info: $affinity_info"
echo "========================="

process_file() {
    local FILE="$1"
    local input_file="$2"
    local max_buffer_size=$((1024 * 1024 * 10))
    declare -A pattern_array
    declare -A buffer_array
    declare -A buffer_size_array

    while IFS= read -r line || [[ -n "$line" ]]; do
        [[ -z "$line" || "$line" =~ ^# ]] && continue
        if [[ "$line" =~ ^([^|]+)\|(.*)$ ]]; then
            output_file="${BASH_REMATCH[1]}"
            patterns="${BASH_REMATCH[2]}"
            IFS='|' read -ra pattern_list <<<"$patterns"
            for pattern in "${pattern_list[@]}"; do
                pattern="${pattern%\"}"
                pattern="${pattern#\"}"
                pattern_array["$pattern"]="$output_file"
                buffer_array["$output_file"]=""
                buffer_size_array["$output_file"]=0
            done
        else
            echo "Invalid line in $input_file: $line"
            continue
        fi
    done <"$input_file"

    if [[ ! -f "$FILE" ]]; then
        echo "File $FILE not found, skipping."
        exit 0
    fi

    echo "Processing file: $FILE"
    shopt -s nocasematch

    while read -r line; do
        for pattern in "${!pattern_array[@]}"; do
            if [[ "$line" =~ $pattern ]]; then
                output_file="${pattern_array[$pattern]}"
                jq_output=$(echo "$line" | jq --arg filename "$FILE" '{text: .text, text_sha: .text_sha, url: .url, url_domain: .url_domain, score: .score, filename: $filename}')
                buffer_array["$output_file"]+="$jq_output"$'\n'
                buffer_size_array["$output_file"]=$((${buffer_size_array["$output_file"]} + ${#jq_output}))
                if ((${buffer_size_array["$output_file"]} >= max_buffer_size)); then
                    (
                        flock -w 10 200 || exit 1
                        echo -n "${buffer_array["$output_file"]}" >&200
                        buffer_array["$output_file"]=""
                        buffer_size_array["$output_file"]=0
                    ) 200>>"$output_file"
                fi
                break
            fi
        done
    done < <(gunzip -c "$FILE")

    for output_file in "${!buffer_array[@]}"; do
        if [[ -n "${buffer_array["$output_file"]}" ]]; then
            exec 200>>"$output_file"
            flock -w 10 200 || exit 1
            echo -n "${buffer_array["$output_file"]}" >&200
            buffer_array["$output_file"]=""
            buffer_size_array["$output_file"]=0
            exec 200>&-
        fi
    done
}

export -f process_file

if command -v parallel >/dev/null; then
    find "$base_folder" -type f -name "*.gz" | parallel -j "$max_threads" process_file "{}" "$input_file"
else
    echo "GNU Parallel is not available. Using background jobs for parallel processing."
    pids=()
    mapfile -t files < <(find "$base_folder" -type f -name "*.gz")
    for FILE in "${files[@]}"; do
        while (($(ps --ppid $$ -o pid= | wc -l) >= max_threads)); do
            sleep 0.1
        done
        process_file "$FILE" "$input_file" &
        pids+=($!)
    done

fi
