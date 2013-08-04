{-# LANGUAGE TypeFamilies, RecordWildCards, RankNTypes, MultiParamTypeClasses,FlexibleInstances,FlexibleContexts,FunctionalDependencies #-}
module Database.KVS (
  KVS (..), WipableKVS (..),
  WrappedKVS,
  wrap, wrapBinary, wrapShow, wrapJSON) where

import Prelude hiding(lookup)

--import Control.Monad.Trans
import Control.Monad.Trans.Resource
import Data.Maybe (fromJust, fromMaybe)
import Data.Default
import qualified Data.ByteString.Lazy as LBS (ByteString) 
import qualified Data.ByteString.Char8 as BS8 (ByteString,pack,unpack)
import qualified Data.Binary as BIN (Binary, encode, decode)
import Data.Conduit (Source, ($=))
import qualified Data.Conduit.List as C (map)

import qualified Data.Aeson as AE (ToJSON, encode, FromJSON, decode)



{-

-}
class (Monad s) => KVS c s k v | c -> s, c -> k, c -> v where
  insert :: c -> k -> v -> s ()
  lookup :: (Functor s) => c -> k -> s (Maybe v)
  lookupWithDefault :: (Functor s, Default v) => c -> k -> s v
  lookupWithDefault k = fmap (fromMaybe def) . lookup k
  delete :: c -> k -> s ()
  accept :: (Functor s) => c -> k -> (Maybe v -> s b) -> s b
  accept c k f = lookup c k >>= f
  elems :: c -> Source (ResourceT s) v
  elems c = elemsWithKey c $= C.map snd
  elemsWithKey :: c -> Source (ResourceT s) (k,v)
  keys :: c -> Source (ResourceT s) k
  keys c = elemsWithKey c $= C.map fst

class (Monad s) => WipableKVS c s | c -> s where
  wipe :: c -> s ()



{-
-}
data WrappedKVS c k' v' (s :: * -> *) k v
  = WrappedKVS {
    fk :: k -> k',
    fk' :: k' -> k,
    fv :: v -> v',
    fv' :: v' -> v,
    wrapped :: c 
  }

instance (KVS c s k' v', Monad s) => KVS (WrappedKVS c k' v' s k v) s k v where
  insert (WrappedKVS {..}) k v = insert wrapped (fk k) (fv v)
  lookup (WrappedKVS {..}) = fmap (fmap fv') . lookup wrapped . fk
  delete (WrappedKVS {..}) = delete wrapped . fk
  accept (WrappedKVS {..}) x f = accept wrapped (fk x) (f . fmap fv')
  elems (WrappedKVS {..}) = elems wrapped $= C.map fv'
  elemsWithKey (WrappedKVS {..}) = elemsWithKey wrapped $= C.map (\(x,y) -> (fk' x, fv' y))

instance (Monad s, WipableKVS c s) => WipableKVS (WrappedKVS c k' v' s k v) s where
  wipe (WrappedKVS {..}) = wipe wrapped
  

wrap :: KVS a s k' v' => (k -> k') -> (k' -> k) -> (v -> v') -> (v' -> v) -> a -> WrappedKVS a k' v' s k v
wrap = WrappedKVS

wrapBinary :: (KVS a s LBS.ByteString LBS.ByteString, BIN.Binary k, BIN.Binary v) => a -> WrappedKVS a LBS.ByteString LBS.ByteString s k v
wrapBinary = WrappedKVS BIN.encode BIN.decode BIN.encode BIN.decode

wrapShow :: (KVS a s LBS.ByteString LBS.ByteString, Show k, Show v, Read k, Read v) => a -> WrappedKVS a BS8.ByteString BS8.ByteString s k v
wrapShow = WrappedKVS (BS8.pack . show) (read . BS8.unpack) (BS8.pack . show) (read . BS8.unpack)

wrapJSON :: (KVS a s LBS.ByteString LBS.ByteString, AE.FromJSON k, AE.ToJSON k, AE.FromJSON v, AE.ToJSON v) => a -> WrappedKVS a LBS.ByteString LBS.ByteString s k v
wrapJSON = WrappedKVS AE.encode (fromJust . AE.decode) AE.encode (fromJust . AE.decode)


