{ pkgs ? import <nixpkgs> {} }:
pkgs.mkShell {
    nativeBuildInputs = with pkgs; [
        gcc 
        bison
        flex
        ninja
        clang-tools
        cmake
        gtest
        fmt
        linuxKernel.packages.linux_zen.perf
        valgrind
        tbb
        openssl
        gdb
    ];
}

