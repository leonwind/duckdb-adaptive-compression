{ pkgs ? import <nixpkgs> {} }:
let 
    python = pkgs.python3;
    python-packages = python.withPackages (p: with p; [
        seaborn
        matplotlib 
    ]);
in pkgs.mkShell {
    nativeBuildInputs = with pkgs; [
        python-packages
        texlive.combined.scheme-full
    ];
}

