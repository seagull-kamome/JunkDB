{-# LANGUAGE OverloadedStrings,FlexibleContexts #-}

import Prelude hiding (lookup)
import Control.Monad (when)
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Resource
import Data.Text (Text)
import Data.String
import System.Directory
import Test.Hspec
import Test.HUnit (assertBool, assertEqual, assertFailure, Assertion)

import qualified Data.HashTable.IO as HT

import Database.KVS
import Database.Junk.HashTable
import Database.Junk.FileSystem
import Database.Junk.GDBM
import Database.Junk.Memcached

main = do
  (HT.new :: IO (HT.BasicHashTable Text Text)) 
    >>= hspec . describe "HashTables" . spec . HT
  --FileSystemKVS "./test"
  --  >>= hspec . describe "FileSystem" . spec . wrap 
  runResourceT $ do
    lift $ do
      b <- doesFileExist "./test/test.db"
      when b $ removeFile "./test/test.db"
    withNewDB "./test/test.db" 0 0o666 (lift . hspec . describe "GDBM" . spec)
  return ()

spec :: (IsString k, IsString v, Eq k, Eq v, Show k, Show v, KVS a IO k v) => a -> Spec
spec c = do
  it "insert" $ do
    insert c "foo" "banana"
    insert c "bar" "apple"
  it "lookup" $ do
    lookup c "foo" >>= assertEqual "foo" (Just "banana")
    lookup c "bar" >>= assertEqual "bar" (Just "apple")
  it "lookup non exist element" $ do
    lookup c "baz" >>= assertEqual "baz" Nothing
  it "delete" $ do
    delete c "foo"
    lookup c "foo" >>= assertEqual "foo deleted" Nothing
     
