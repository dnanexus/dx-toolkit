main() {
    dx-download-all-inputs

    # check if subject_claims is an empty string
    if [ -z "$subject_claims" ]; then
        token=$(dx-jobutil-get-identity-token --aud "$audience")
    else
        token=$(dx-jobutil-get-identity-token --aud "$audience" --subject_claims "$subject_claims")
    fi

    dx-jobutil-add-output token "$token"
}
