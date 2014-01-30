{-# LANGUAGE TypeFamilies, RecordWildCards, RankNTypes, MultiParamTypeClasses,FlexibleInstances,FlexibleContexts,FunctionalDependencies,DefaultSignatures,GADTs,UnicodeSyntax,OverloadedStrings #-}
module Database.KVS (
  KVS (..), EnumeratableKVS(..), WipableKVS (..),
  BucketKVS(..), WrappedKVS,
  lookupWithDefault,
  wrap, wrapBinary, wrapShow, wrapJSON) where

import Prelude hiding(lookup)

import Control.Monad.Trans (lift)
import Control.Monad.Trans.Resource
import Data.Maybe (fromJust, fromMaybe)
import Data.Default
import qualified Data.ByteString.Lazy as LBS (ByteString,append)
import qualified Data.ByteString.Char8 as BS8 (ByteString,pack,unpack)
import qualified Data.Binary as BIN (Binary, encode, decode)
import Data.Conduit (Source, ($=), await, yield)
import qualified Data.Conduit.List as C (map)

import qualified Data.Aeson as AE (ToJSON, encode, FromJSON, decode)



{-

-}
class (Monad s) => KVS c s k v | c -> s, c -> k, c -> v where
  insert :: c -> k -> v -> s ()
  
  lookup :: c -> k -> s (Maybe v)
  lookup c k = accept c k (return Nothing) (return . Just)
  
  -- | Delete specified key-value pair from container.
  delete :: c                   -- ^ Container
            -> k                -- ^ Key
            -> s (Maybe Bool)   -- ^ success, failed or unknown

  -- | Lookup value
  accept :: c                   -- ^ Container
            -> k                -- ^ Key
            -> s b              -- ^ Action for key not found
            -> (v -> s b)       -- ^ Action for key found (with Lock)
            -> s b
  
class Monad s => EnumeratableKVS c s k v | c -> s, c -> k, c-> v where
  keys :: c -> Source (ResourceT s) k
  keys c = elemsWithKey c $= C.map fst
  elems :: c -> Source (ResourceT s) v
  elems c = elemsWithKey c $= C.map snd
  elemsWithKey :: c -> Source (ResourceT s) (k,v)
  default elemsWithKey :: KVS c s k v => c -> Source (ResourceT s) (k,v)
  elemsWithKey c = keys c $= go where
    go = do 
      k <- await
      case k of
        Nothing -> return ()
        Just k' -> do
          v <- lift $ lift $ lookup c k'
          case v of
            Nothing -> go
            Just v' -> yield (k',v') >> go

class (Monad s) => WipableKVS c s | c -> s where
  wipe :: c -> s ()



lookupWithDefault :: (Monad s, Default v, KVS c s k v) => c -> k -> s v
lookupWithDefault c k = lookup c k >>= return . fromMaybe def
{-# INLINE lookupWithDefault #-}  


{-
-}
data BucketKVS c s v where
  BucketKVS :: KVS c s LBS.ByteString v ⇒ LBS.ByteString → c → BucketKVS c s v

wrapNamespace ∷ LBS.ByteString → LBS.ByteString → LBS.ByteString
wrapNamespace ns k = ns `LBS.append` "::" `LBS.append` k


instance Monad s ⇒ KVS (BucketKVS c s v) s LBS.ByteString v where
  insert (BucketKVS b x) k v = insert x (wrapNamespace b k) v
  delete (BucketKVS b x) = delete x . wrapNamespace b
  accept (BucketKVS b x) = accept x . wrapNamespace b

instance (Monad s,WipableKVS c s) ⇒ WipableKVS (BucketKVS c s v) s where
  wipe (BucketKVS _ x) = wipe x
--instance (Monad s, EnumeratableKVS c s k v) ⇒ EnumeratableKVS (BucketKVS c s k) s k v where -- キーからプレフィックスを剥がすのが面倒いですよ
  
  

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
  lookup (WrappedKVS {..}) k =
    lookup wrapped (fk k) >>= return . maybe Nothing (Just . fv')
  delete (WrappedKVS {..}) = delete wrapped . fk
  accept (WrappedKVS {..}) k f g = accept wrapped (fk k) f (g . fv')

instance (Monad s, WipableKVS c s) => WipableKVS (WrappedKVS c k' v' s k v) s where
  wipe (WrappedKVS {..}) = wipe wrapped
  
instance (Monad s, EnumeratableKVS c s k' v') => EnumeratableKVS (WrappedKVS c k' v' s k v) s k v where
  elems (WrappedKVS {..}) = elems wrapped $= C.map fv'
  elemsWithKey (WrappedKVS {..}) = elemsWithKey wrapped $= C.map (\(x,y) -> (fk' x, fv' y))
  

wrap :: KVS a s k' v' => (k -> k') -> (k' -> k) -> (v -> v') -> (v' -> v) -> a -> WrappedKVS a k' v' s k v
wrap = WrappedKVS

wrapBinary :: (KVS a s LBS.ByteString LBS.ByteString, BIN.Binary k, BIN.Binary v) => a -> WrappedKVS a LBS.ByteString LBS.ByteString s k v
wrapBinary = WrappedKVS BIN.encode BIN.decode BIN.encode BIN.decode

wrapShow :: (KVS a s LBS.ByteString LBS.ByteString, Show k, Show v, Read k, Read v) => a -> WrappedKVS a BS8.ByteString BS8.ByteString s k v
wrapShow = WrappedKVS (BS8.pack . show) (read . BS8.unpack) (BS8.pack . show) (read . BS8.unpack)

wrapJSON :: (KVS a s LBS.ByteString LBS.ByteString, AE.FromJSON k, AE.ToJSON k, AE.FromJSON v, AE.ToJSON v) => a -> WrappedKVS a LBS.ByteString LBS.ByteString s k v
wrapJSON = WrappedKVS AE.encode (fromJust . AE.decode) AE.encode (fromJust . AE.decode)



