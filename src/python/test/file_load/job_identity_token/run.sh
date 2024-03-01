main() {
    dx-download-all-inputs

    token=$(dx-jobutil-get-identity-token --aud "$audience" --subject_claims "$subject_claims")

    dx-jobutil-add-output token "$token"
}
