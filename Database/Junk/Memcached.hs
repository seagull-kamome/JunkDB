{-# LANGUAGE OverloadedStrings, TypeFamilies, RankNTypes,TypeSynonymInstances, MultiParamTypeClasses,FlexibleInstances,RecordWildCards,ViewPatterns,LambdaCase #-}
module Database.Junk.Memcached (connect, disconnect, Memcached (..)) where

import Control.Applicative
import qualified Data.ByteString.Char8 as BS
import qualified Network as N
import System.IO (Handle, hClose,hFlush)

import Database.KVS

data Memcached = Memcached { mcdServer :: Handle, mcdExpires :: Int, mcdFlags :: Int }

connect :: N.HostName -> N.PortNumber -> Int -> Int -> IO Memcached
connect hn pn expires flags= do
  h <- N.connectTo hn (N.PortNumber pn)
  return $ Memcached h expires flags

disconnect :: Memcached -> IO ()
disconnect = hClose . mcdServer

instance KVS Memcached IO BS.ByteString BS.ByteString where
  insert (Memcached {..}) k v =
    mapM_ (BS.hPut mcdServer) ["set ", k, " " , BS.pack $ show mcdFlags, " ", BS.pack $ show mcdFlags, " ", BS.pack $ show (BS.length v), "\r\n", v, "\r\n"]
    <* BS.hGetLine mcdServer
  accept (Memcached {..}) k f g = do
    mapM_ (BS.hPut mcdServer) ["get ", k, "\r\n"]
    hFlush mcdServer
    BS.words <$> BS.hGetLine mcdServer >>= \case
      ["VALUE", _,_, read . BS.unpack -> n] -> BS.hGet mcdServer n <* BS.hGetLine mcdServer <* BS.hGetLine mcdServer >>= g
      _ -> f
  delete (Memcached {..}) k = do
    mapM_ (BS.hPut mcdServer) ["delete ", k, " 0"]
    hFlush mcdServer
    BS.hGetLine mcdServer >>= return . Just . (== "DELETED\r")
