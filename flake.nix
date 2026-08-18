{
  description = "Development environment for db-sync (Hybrid Nix + Venv)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      supportedSystems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forAllSystems = nixpkgs.lib.genAttrs supportedSystems;
    in
    {
      devShells = forAllSystems (system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in
        {
          default = pkgs.mkShell {
            packages = with pkgs; [
              python311
              python311Packages.pip
              python311Packages.virtualenv
              libpq
              zlib
            ];

            shellHook = ''
              if [ ! -d ".venv" ]; then
                echo "🌱 Creating Virtual Environment..."
                python -m venv .venv
              fi

              source .venv/bin/activate

              if [ -f "requirements.txt" ]; then
                pip install -q -r requirements.txt
              fi

              echo "🐍 db-sync environment activated! (venv + requirements.txt)"
            '';
          };
        }
      );
    };
}