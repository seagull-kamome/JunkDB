{-# LANGUAGE OverloadedStrings,TypeFamilies,MultiParamTypeClasses,FlexibleInstances #-}
module Database.Junk.FileSystem (
  FileSystemKVS (..)
  ) where

import Control.Monad.Trans (lift)
import qualified Data.ByteString as BS
import System.FilePath
import System.Directory
import Data.Conduit (yield, ($=))
import qualified Data.Conduit.List as C (concatMapM,mapM,filter)

import Database.KVS

newtype FileSystemKVS
  = FileSystemKVS {
    fsBasePath :: FilePath
    }

instance KVS (FileSystemKVS) IO FilePath BS.ByteString where
  insert (FileSystemKVS dir) k v = BS.writeFile (dir </> k) v
  accept (FileSystemKVS dir) k f g = do
    b <- doesFileExist (dir </> k)
    if b 
      then f
      else BS.readFile (dir </> k) >>= g
  delete (FileSystemKVS dir) k = removeFile (dir </> k) >> return (Just True)
  elemsWithKey c@(FileSystemKVS dir) = 
    keys c $= C.mapM (\x -> do
                         y <- lift $ BS.readFile (dir </> x)
                         return (x,y))
  keys (FileSystemKVS dir) =
    yield dir 
    $= C.concatMapM (lift . getDirectoryContents)
    $= C.filter (not . flip elem [".",".."])

instance WipableKVS (FileSystemKVS) IO where
  wipe (FileSystemKVS dir) =
    getDirectoryContents dir >>= mapM_ (removeFile . (dir </>))
