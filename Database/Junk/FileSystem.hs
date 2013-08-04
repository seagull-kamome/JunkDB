{-# LANGUAGE OverloadedStrings,TypeFamilies,MultiParamTypeClasses,FlexibleInstances #-}
module Database.Junk.FileSystem (
  FileSystemKVS (..)
  ) where

import Control.Monad.Trans (lift)
import qualified Data.ByteString.Lazy as LBS
import System.FilePath
import System.Directory
import Data.Conduit (yield, ($=))
import qualified Data.Conduit.List as C (concatMapM,mapM,filter)

import Database.KVS

newtype FileSystemKVS
  = FileSystemKVS {
    fsBasePath :: FilePath
    }

instance KVS (FileSystemKVS) IO FilePath LBS.ByteString where
  insert (FileSystemKVS dir) k v = LBS.writeFile (dir </> k) v
  lookup (FileSystemKVS dir) k = do
    b <- doesFileExist (dir </> k)
    if b 
      then return Nothing
      else LBS.readFile (dir </> k) >>= return . Just
  delete (FileSystemKVS dir) k = removeFile (dir </> k)
  elemsWithKey c@(FileSystemKVS dir) = 
    keys c $= C.mapM (\x -> do
                         y <- lift $ LBS.readFile (dir </> x)
                         return (x,y))
  keys (FileSystemKVS dir) =
    yield dir 
    $= C.concatMapM (lift . getDirectoryContents)
    $= C.filter (not . flip elem [".",".."])

instance WipableKVS (FileSystemKVS) IO where
  wipe (FileSystemKVS dir) =
    getDirectoryContents dir >>= mapM_ (removeFile . (dir </>))
