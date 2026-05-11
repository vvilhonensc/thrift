build-thrift:
    #!/usr/bin/env bash
    set -euo pipefail
    export PATH="/opt/homebrew/opt/bison/bin:/opt/homebrew/opt/flex/bin:$PATH"
    cmake -S . -B build/thrift-compiler \
        -DBUILD_COMPILER=ON \
        -DBUILD_LIBRARIES=OFF \
        -DBUILD_TESTING=OFF \
        -DBUILD_TUTORIALS=OFF \
        -DCMAKE_BUILD_TYPE=Release
    cmake --build build/thrift-compiler -j
    mkdir -p bin
    cp build/thrift-compiler/compiler/cpp/bin/thrift bin/thrift
