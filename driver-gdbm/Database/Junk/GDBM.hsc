{-# LANGUAGE ForeignFunctionInterface,OverloadedStrings,TypeFamilies,MultiParamTypeClasses,FlexibleInstances,EmptyDataDecls #-}
module Database.Junk.GDBM (
  version,
  withReader, withWriter, withNewDB
  ) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BS
import Data.Conduit (yield)

import Data.Word

import Control.Applicative
import Control.Monad (void)
import Control.Monad.Trans.Resource
import Control.Monad.Trans (lift)

import Foreign.C.Types (CInt,CChar)
import Foreign.C.String (newCString, CString)
import Foreign.Marshal.Alloc (free, finalizerFree,alloca)
import Foreign.Ptr (nullPtr, nullFunPtr, FunPtr, Ptr, castPtr)
import Foreign.ForeignPtr
import Foreign.Storable

import Database.KVS

#include <gdbm.h>

data GDBMFile
newtype GDBM = GDBM { unGDBM:: (Ptr GDBMFile) }


withNewDB :: FilePath -> Int -> Int -> (GDBM -> ResourceT IO a) -> ResourceT IO a
withNewDB fs bs mode f = withGDBM fs bs (#const GDBM_NEWDB) mode f
{-# INLINE withNewDB #-}


withReader :: FilePath -> Int -> Int -> (GDBM -> ResourceT IO a) -> ResourceT IO a
withReader fs bs mode f = withGDBM fs bs (#const GDBM_READER) mode f 
{-# INLINE withReader #-}


withWriter :: FilePath -> Int -> Int -> Bool -> (GDBM -> ResourceT IO a) -> ResourceT IO a
withWriter fs bs mode cr f = withGDBM fs bs (if cr then (#const GDBM_WRCREAT) else (#const GDBM_WRITER)) mode f
{-# INLINE withWriter #-}


withGDBM :: FilePath -> Int -> Int -> Int -> (GDBM -> ResourceT IO a) -> ResourceT IO a
withGDBM fs bs flags mode f = do
  (_, fs') <- allocate (newCString fs) free
  (_, h) <- allocate 
            (gdbm_open fs' (fromIntegral bs) flags (fromIntegral mode) nullFunPtr)
            (gdbm_close)
  f $ GDBM h



instance KVS GDBM IO BS.ByteString BS.ByteString where
  insert (GDBM dbf) k v = 
    BS.unsafeUseAsCStringLen k $ \(kptr,klen) -> 
    BS.unsafeUseAsCStringLen v $ \(vptr,vlen) -> 
    void $ gdbm_store dbf klen kptr vlen vptr (#const GDBM_REPLACE)
  lookup (GDBM dbf) k = 
    BS.unsafeUseAsCStringLen k $ \(kptr,klen) ->
    alloca $ \vlen -> do
      vptr <- gdbm_fetch dbf klen kptr vlen
      if vptr == nullPtr 
        then return Nothing
        else do vlen' <- peek vlen
                Just <$> BS.unsafePackCStringFinalizer (castPtr vptr) vlen' (free vptr)
  delete (GDBM dbf) k =
    BS.unsafeUseAsCStringLen k $ \(kptr,klen) -> void $ gdbm_delete dbf klen kptr
  keys (GDBM dbf) = do
    (klen, kptr) <- lift $ lift $ alloca $ \klen -> do
      kptr <- gdbm_firstkey dbf klen
      klen' <- peek klen
      return (klen', kptr)
    if kptr == nullPtr
      then return ()
      else (lift $ lift $
            BS.unsafePackCStringFinalizer (castPtr kptr) klen (free kptr))
           >>= yield
           >> go klen kptr
    where
      go klen kptr = do
        (new_klen, new_kptr) <- lift $ lift $ alloca $ \new_klen -> do
          new_kptr <- gdbm_nextkey dbf klen kptr new_klen
          new_klen' <- peek new_klen
          return (new_klen', new_kptr)
        if new_kptr == nullPtr
          then return ()
          else (lift $ lift $
                BS.unsafePackCStringFinalizer (castPtr new_kptr) new_klen (free new_kptr))
               >>= yield
               >> go new_klen new_kptr
               

foreign import ccall "gdbm.h &gdbm_version" version :: CString
foreign import ccall "gdbm.h gdbm_open" gdbm_open :: CString -> Int -> Int -> Int -> FunPtr (IO ()) -> IO (Ptr GDBMFile)
foreign import ccall "gdbm.h gdbm_close" gdbm_close :: Ptr GDBMFile -> IO ()
foreign import ccall "junk_gdbm_store" gdbm_store :: Ptr GDBMFile -> Int -> Ptr CChar -> Int -> Ptr CChar -> Int -> IO Int
foreign import ccall "junk_gdbm_fetch" gdbm_fetch :: Ptr GDBMFile -> Int -> Ptr CChar -> Ptr Int -> IO (Ptr CChar)
foreign import ccall "junk_gdbm_delete" gdbm_delete :: Ptr GDBMFile -> Int -> Ptr CChar -> IO Int
foreign import ccall "junk_gdbm_firstkey" gdbm_firstkey :: Ptr GDBMFile -> Ptr Int -> IO (Ptr CChar)
foreign import ccall "junk_gdbm_nextkey" gdbm_nextkey :: Ptr GDBMFile -> Int -> Ptr CChar -> Ptr Int -> IO (Ptr CChar)



