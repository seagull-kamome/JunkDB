{-# LANGUAGE TypeFamilies, RankNTypes,TypeSynonymInstances, MultiParamTypeClasses,FlexibleInstances #-}
module Database.Junk.HashTable (HT (..)) where

import Control.Monad.ST.Safe
import qualified Data.Hashable as HS (Hashable)
import qualified Data.HashTable.Class as H (HashTable)
import qualified Data.HashTable.IO as H
import Data.Conduit (yield, ($=))
import qualified Data.Conduit.List as C (concatMapM)

import Database.KVS

newtype HT h (s :: * -> *) k v = HT { unHT :: (H.IOHashTable h k v) }

instance (H.HashTable h, HS.Hashable k) => KVS (HT h) IO k v where
  insert = H.insert . unHT
  lookup = H.lookup . unHT
  delete = H.delete . unHT
  elemsWithKey (HT c) = yield c $= C.concatMapM H.toList

