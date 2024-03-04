main() {
    dx-download-all-inputs

    # check if subject_claims is an empty string
    if [ -z "$subject_claims" ]; then
        subject_claims=
    fi

    token=$(dx-jobutil-get-identity-token --aud "$audience" --subject_claims "$subject_claims")

    dx-jobutil-add-output token "$token"
}
