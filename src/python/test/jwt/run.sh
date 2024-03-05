main() {
    set -x
    dx-download-all-inputs

    # check if subject_claims is an empty string
    if [ -z "$subject_claims" ]; then
        token=$(dx-jobutil-get-identity-token --aud "$audience")
    else
        subject_claims=$(printf ",%s" "${subject_claims[@]}")
        subject_claims=${subject_claims:1}
        token=$(dx-jobutil-get-identity-token --aud "$audience" --subject_claims "$subject_claims")
    fi

    dx-jobutil-add-output token "$token"
}
