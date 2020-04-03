main() {
    echo "(wjk) starting..."

    curl -O https://github.com/dnanexus/dxfuse/releases/download/v0.21/dxfuse-linux
    mv dxfuse-linux dxfuse

    ls dxfuse
    ls .
    ls /usr/bin

    dx-mount-all-inputs

    find /home/dnanexus/in -name \*

    echo "(wjk) done!"
}
