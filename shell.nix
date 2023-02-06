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
        linuxKernel.packages.linux_5_18.perf
        valgrind
        tbb
        openssl
    ];
}

