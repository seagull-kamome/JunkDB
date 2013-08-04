{-# LANGUAGE TypeFamilies, RankNTypes,TypeSynonymInstances, MultiParamTypeClasses,FlexibleInstances #-}
module Database.Junk.HashTable (HT (..)) where

import Control.Monad.Trans (lift)
import qualified Data.Hashable as HS (Hashable)
import qualified Data.HashTable.Class as H (HashTable)
import qualified Data.HashTable.IO as H
import Data.Conduit (yield, ($=))
import qualified Data.Conduit.List as C (concatMapM)

import Database.KVS

newtype HT h k v = HT { unHT :: (H.IOHashTable h k v) }

instance (Eq k, H.HashTable h, HS.Hashable k) => KVS (HT h k v) IO k v where
  insert = H.insert . unHT
  lookup = H.lookup . unHT
  delete = H.delete . unHT
  elemsWithKey (HT c) = yield c $= C.concatMapM (lift . H.toList)

