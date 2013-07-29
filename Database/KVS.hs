{-# LANGUAGE TypeFamilies, RecordWildCards, RankNTypes, MultiParamTypeClasses,FlexibleInstances #-}
module Database.KVS (
  KVS (..),
  WrappedKVS,
  wrapBinary, wrapShow, wrapJSON) where

import Prelude hiding(lookup)
import Data.Maybe (fromJust, fromMaybe)
import Data.Default
import qualified Data.ByteString.Lazy as LBS (ByteString) 
import qualified Data.ByteString.Char8 as BS8 (ByteString,pack,unpack)
import qualified Data.Binary as BIN (Binary, encode, decode)
import Data.Conduit (Source, ($=), ($$))
import qualified Data.Conduit.List as C (map, mapM_)

import qualified Data.Aeson as AE (ToJSON, encode, FromJSON, decode)



{-
-}
class KVS c s k v where
  insert :: (Eq k) => c s k v -> k -> v -> s ()
  lookup :: (Eq k, Functor s) => c s k v -> k -> s (Maybe v)
  lookupWithDefault :: (Eq k, Functor s, Default v) => c s k v -> k -> s v
  lookupWithDefault k = fmap (fromMaybe def) . lookup k
  delete :: (Eq k) => c s k v -> k -> s ()
  accept :: (Eq k, Functor s, Monad s) => c s k v -> k -> (Maybe v -> s b) -> s b
  accept c k f = lookup c k >>= f
  erase :: (Eq k, Monad s) => c s k v -> s ()
  erase x = elemsWithKey x $$ C.mapM_ (delete x . fst)
  elems :: (Eq k, Monad s) => c s k v -> Source s v
  elems c = elemsWithKey c $= C.map snd
  elemsWithKey :: (Eq k, Monad s) => c s k v -> Source s (k,v)
  keys :: (Eq k, Monad s) => c s k v -> Source s k
  keys c = elemsWithKey c $= C.map fst


{-
-}
data WrappedKVS c k' v' (s :: * -> *) k v
  = WrappedKVS {
    fk :: k -> k',
    fk' :: k' -> k,
    fv :: v -> v',
    fv' :: v' -> v,
    wrapped :: c s k' v'
  }

instance (KVS c s k' v', Functor s, Eq k') => KVS (WrappedKVS c k' v') s k v where
  insert (WrappedKVS {..}) k v = insert wrapped (fk k) (fv v)
  lookup (WrappedKVS {..}) = fmap (fmap fv') . lookup wrapped . fk
  delete (WrappedKVS {..}) = delete wrapped . fk
  accept (WrappedKVS {..}) x f = accept wrapped (fk x) (f . fmap fv')
  elems (WrappedKVS {..}) = elems wrapped $= C.map fv'
  elemsWithKey (WrappedKVS {..}) = elemsWithKey wrapped $= C.map (\(x,y) -> (fk' x, fv' y))

  
wrapBinary :: (KVS a s k v, Eq k, BIN.Binary k, BIN.Binary v) => a s LBS.ByteString LBS.ByteString -> WrappedKVS a LBS.ByteString LBS.ByteString s k v
wrapBinary = WrappedKVS BIN.encode BIN.decode BIN.encode BIN.decode

wrapShow :: (KVS a s k v, Show k, Show v, Read k, Read v) => a s BS8.ByteString BS8.ByteString -> WrappedKVS a BS8.ByteString BS8.ByteString s k v
wrapShow = WrappedKVS (BS8.pack . show) (read . BS8.unpack) (BS8.pack . show) (read . BS8.unpack)

wrapJSON :: (KVS a s k v, AE.FromJSON k, AE.ToJSON k, AE.FromJSON v, AE.ToJSON v) => a s LBS.ByteString LBS.ByteString -> WrappedKVS a LBS.ByteString LBS.ByteString s k v
wrapJSON = WrappedKVS AE.encode (fromJust . AE.decode) AE.encode (fromJust . AE.decode)


