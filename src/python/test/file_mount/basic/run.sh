main() {
    echo "(wjk) starting..."
    ls .
    ls /usr/bin

    curl -O https://github.com/dnanexus/dxfuse/releases/download/v0.21/dxfuse-linux
    mv dxfuse-linux dxfuse
    
    dx-mount-all-inputs

    find /home/dnanexus/in -name \*

    echo "(wjk) done!"
}
