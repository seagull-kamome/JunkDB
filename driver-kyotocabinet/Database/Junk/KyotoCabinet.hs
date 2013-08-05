{-# LANGUAGE OverloadedStrings,TypeFamilies,MultiParamTypeClasses,FlexibleInstances #-}
module Database.Junk.KyotoCabinet (
  KyotoCabinet
  ) where

import Control.Monad (forever)
import Control.Monad.Trans (liftIO)
import qualified Data.ByteString as BS
import qualified Database.KyotoCabinet.Db as KC
import Data.Conduit (yield, ($=))
import Database.KVS


newtype KyotoCabinet (s :: * -> *) k v 
  = KyotoCabinet { unKyotoCabinet :: KC.KcDb }

instance KVS KyotoCabinet IO BS.ByteString BS.ByteString where
  insert = KC.kcdbadd . unKyotoCabinet
  lookup = KC.kcdbget . unKyotoCabinet
  delete = KC.kcdbremove . unKyotoCabinet
  elems (KyotoCabinet kc) = do
    cur <- liftIO $ KC.kcdbcursor kc
    forever $ (liftIO $ KC.kccurgetvalue cur True) >>= yield
    liftIO $ KC.kccurdel cur
  elemsWithKey (KyotoCabinet kc) = do
    cur <- liftIO $ KC.kcdbcursor kc
    forever $ (liftIO $ KC.kccurget cur True) >>= yield
    liftIO $ KC.kccurdel cur
  keys (KyotoCabinet kc) = do
    cur <- liftIO $ KC.kcdbcursor kc
    forever $ (liftIO $ KC.kccurgetkey cur True) >>= yield
    liftIO $ KC.kccurdel cur
