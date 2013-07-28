{-# LANGUAGE TypeFamilies, RankNTypes,TypeSynonymInstances, MultiParamTypeClasses,FlexibleInstances #-}
module Database.KVS.Junk.Memcached (Memcache (..)) where

import Control.Monad (void)
import qualified Network.Memcache.Protocol as MC
import qualified Network.Memcache.Key as MC
import qualified Network.Memcache.Serializable as MC
import qualified Network.Memcache as MC

import Database.KVS

newtype Memcache (s :: * -> *) k v = Memcache { unMemcache :: MC.Server }

instance (MC.Key k, MC.Serializable v) => KVS Memcache IO k v where
  insert (Memcache c) k v = void $ MC.set c k v
  lookup = MC.get . unMemcache
  delete (Memcache c) k = void $ MC.delete c k 0
  elemsWithKey = undefined
