cabal --force-reinstall install
(cd driver-gdbm; cabal --force-reinstall --sandbox-config-file=../cabal.sandbox.config install; cabal sdist)
(cd driver-hashtables; cabal --force-reinstall --sandbox-config-file=../cabal.sandbox.config install; cabal sdist)

