#!/bin/bash

# FOR BRANCH TEST, UNDO LATER
install_dxpy_from_branch() {
  dxpy_git_ref='job_id_tokens_debug_subject_claims'
  pip3 install --upgrade "git+https://github.com/dnanexus/dx-toolkit.git@${dxpy_git_ref}#egg=dxpy&subdirectory=src/python"
  dx --version
}

main() {
    install_dxpy_from_branch
    dx-download-all-inputs

    # check if subject_claims is an empty string
    if [ -z "$subject_claims" ]; then
        token=$(dx-jobutil-get-identity-token --aud "$audience")
    else
        token=$(dx-jobutil-get-identity-token --aud "$audience" --subject_claims "$subject_claims")
    fi

    dx-jobutil-add-output token "$token"
}
