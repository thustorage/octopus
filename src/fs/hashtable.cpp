/*** Hash table class for index in file system. ***/

/** Version 2 + Crypto++ support. **/

/* FIXME: Ignore malloc failure. Do not examine the length of path. */

/** Included files. **/
#include "hashtable.hpp"
/** Global variable. **/
// SHA256 sha256;                          /* FIXME: Is SHA256 class thread-safe? */

/* Get address hash of specific string. No check of parameter here.
   @param   buf         Buffer of original data.
   @param   len         Length of buffer.
   @param   hashAddress Buffer of address hash. */
void HashTable::getAddressHash(const char *buf, uint64_t len, AddressHash *hashAddress)
{
    uint64_t sha256_buf[4];
    // SHA256 sha256;
    SHA256 sha256;
    sha256.CalculateDigest((byte *)sha256_buf, (const byte *)buf, len);
    // SHA256_CTX ctx;                     /* Context. */
    // sha256_init(&ctx);                  /* Initialize SHA-256 context. */
    // sha256_update(&ctx, (const BYTE *)buf, len); /* Update SHA-256 context. */
    // sha256_final(&ctx, (BYTE *)sha256_buf); /* Finalize SHA-256 context. */
    *hashAddress = sha256_buf[0] & 0x00000000000FFFFF; /* Address hash. Get 20 bits. */
}

/* Get address hash of specific string by unique hash.
   @param   hashUnique  Unique hash.
   @return              Address hash. */
AddressHash HashTable::getAddressHash(UniqueHash *hashUnique)
{
    return hashUnique->value[0] & 0x00000000000FFFFF; /* Address hash. Get 20 bits. */
}

/* Get unique hash of specific string. No check of parameter here.
   @param   buf         Buffer of original data.
   @param   len         Length of buffer.
   @param   hashUnique  Buffer of unique hash. */
void HashTable::getUniqueHash(const char *buf, uint64_t len, UniqueHash *hashUnique)
{
    // SHA256 sha256;
    SHA256 sha256;
    sha256.CalculateDigest((byte *)hashUnique, (const byte *)buf, len);
    // SHA256_CTX ctx;                     /* Context. */
    // sha256_init(&ctx);                  /* Initialize SHA-256 context. */
    // sha256_update(&ctx, (const BYTE *)buf, len); /* Update SHA-256 context. */
    // sha256_final(&ctx, (BYTE *)hashUnique); /* Finalize SHA-256 context. */
}

/* Get a chained item. Check unique hash to judge if chained item is right or not. 
   @param   path        Path.
   @param   indexMeta   Buffer of meta index. 
   @param   isDirectory Buffer to judge if item is directory or not.
   @return              If item does not exist or other error occurs, then return false. Otherwise return true. */
bool HashTable::get(const char *path, uint64_t *indexMeta, bool *isDirectory)
{
    if ((path == NULL) || (indexMeta == NULL) || (isDirectory == NULL)) {
        return false;                   /* Fail due to null parameters. */
    } else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        // printf("%016x%016x%016x%016x\n", hashUnique.value[3], hashUnique.value[2], hashUnique.value[1], hashUnique.value[0]);
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        // getAddressHash(path, strlen(path), &hashAddress); /* Get address hash. */
        bool result;
        mutexBitmapChainedItems.lock(); /* Though currently there is no bitmap reading or writing, other operations such as delete might affect hash item reading. */
        {
            uint64_t indexHead = itemsHash[hashAddress].indexHead;
            if (indexHead == 0) {
                result = false;         /* Fail due to no hash item. */
            } else {
                // UniqueHash hashUnique;
                // getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
                uint64_t indexCurrent = indexHead; /* Index of current chained item. */
                bool found = false;
                do {                    /* Traverse every chained item. */
                    if (memcmp(&(itemsChained[indexCurrent].hashUnique), &(hashUnique), sizeof(UniqueHash)) == 0) {
                        *indexMeta = itemsChained[indexCurrent].indexMeta; /* Save meta index. */
                        *isDirectory = itemsChained[indexCurrent].isDirectory; /* Save state of directory. */
                        found = true;   /* Found one matched. */
                        break;          /* Jump out. */
                    } else {
                        indexCurrent = itemsChained[indexCurrent].indexNext; /* Move to next chained item. */
                    }
                } while (indexCurrent != 0); /* If current item is over last chained item then jump out. */
                if (found == true) {
                    result = true;      /* Succeed. Find one matched. */
                } else {
                    result = false;     /* Fail due to no chained item matched. */
                }
            }
        }
        mutexBitmapChainedItems.unlock(); /* Unlock hash table bitmap. */
        return result;                  /* Return specific result. */
    }
}

/* Get a chained item by hash. Check unique hash to judge if chained item is right or not. 
   @param   hashUnique  Unique hash.
   @param   indexMeta   Buffer of meta index. 
   @param   isDirectory Buffer to judge if item is directory or not.
   @return              If item does not exist or other error occurs, then return false. Otherwise return true. */
bool HashTable::get(UniqueHash *hashUnique, uint64_t *indexMeta, bool *isDirectory)
{
    if ((hashUnique == NULL) || (indexMeta == NULL) || (isDirectory == NULL)) {
        return false;                   /* Fail due to null parameters. */
    } else {
    // UniqueHash hashUnique;
    // getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
    AddressHash hashAddress = HashTable::getAddressHash(hashUnique); /* Get address hash by unique hash. */
    // getAddressHash(path, strlen(path), &hashAddress); /* Get address hash. */
    bool result;
        mutexBitmapChainedItems.lock(); /* Though currently there is no bitmap reading or writing, other operations such as delete might affect hash item reading. */
        {
            uint64_t indexHead = itemsHash[hashAddress].indexHead;
            // Debug::debugItem("get hashAddress = %d, indexHead = %d", hashAddress, indexHead);
            if (indexHead == 0) {
                result = false;         /* Fail due to no hash item. */
            } else {
                // UniqueHash hashUnique;
                // getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
                uint64_t indexCurrent = indexHead; /* Index of current chained item. */
                bool found = false;
                do {                    /* Traverse every chained item. */
                    //printf("5.0 indexCurrent = %d, itemsChained = %lx\n", indexCurrent, itemsChained);
                    //printf("5.0 indexCurrent = %d, %lx\n", (int)indexCurrent, itemsChained[indexCurrent].hashUnique.value[0]);
                    if (memcmp(&(itemsChained[indexCurrent].hashUnique), hashUnique, sizeof(UniqueHash)) == 0) {
                        *indexMeta = itemsChained[indexCurrent].indexMeta; /* Save meta index. */
                        *isDirectory = itemsChained[indexCurrent].isDirectory; /* Save state of directory. */
                        found = true;   /* Found one matched. */
                        break;          /* Jump out. */
                    } else {
                        indexCurrent = itemsChained[indexCurrent].indexNext; /* Move to next chained item. */
                    }
                } while (indexCurrent != 0); /* If current item is over last chained item then jump out. */
                if (found == true) {
                    result = true;      /* Succeed. Find one matched. */
                } else {
                    result = false;     /* Fail due to no chained item matched. */
                }
            }
        }
        // Debug::debugItem("comp hashAddress = %d, indexMeta = %d", hashAddress, *indexMeta);
        mutexBitmapChainedItems.unlock(); /* Unlock hash table bitmap. */
        return result;                  /* Return specific result. */
    }
}

/* Put a chained item. If item has already existed, old meta index will be replaced by the new one. 
   @param   path        Path.
   @param   indexMeta   Meta index to put in. 
   @param   isDirectory Judge if item is directory.
   @return              If error occurs return false, otherwise return true. */
bool HashTable::put(const char *path, uint64_t indexMeta, bool isDirectory)
{
    if (path == NULL) {
        return false;                   /* Fail due to null path. */
    } else {
        AddressHash hashAddress;
        HashTable::getAddressHash(path, strlen(path), &hashAddress); /* Get address hash. */
        bool result;
        mutexBitmapChainedItems.lock(); /* Lock hash table bitmap. */
        {
            uint64_t indexHead = itemsHash[hashAddress].indexHead;
            if (indexHead == 0) {       /* If there is no item currently. */
                // Debug::debugItem("No item currently.");
                uint64_t index;
                // if (bitmapChainedItems->findFree(&index) == false) { /* Method of bitmap search.
                if (headFreeBit == NULL) { /* Method of free bit chain. */
                    result = false;     /* Fail due to no free bit in bitmap. */
                } else {
                    index = headFreeBit->position; /* Get free bit index. */
                    FreeBit *currentFreeBit = headFreeBit; /* Get current free bit. */
                    headFreeBit = (FreeBit *)(headFreeBit->nextFreeBit); /* Move current free bit out of free bit chain. */
                    free(currentFreeBit); /* Release current free bit as used. */
                    if (bitmapChainedItems->set(index) == false) { /* Occupy the position first. Need not to roll back. */ 
                        result = false; /* Fail due to bitmap set error. */
                    } else {
                        itemsChained[index].indexNext = 0; /* Next item does not exist. */
                        itemsChained[index].indexMeta = indexMeta; /* Assign specific meta index. */
                        itemsChained[index].isDirectory = isDirectory; /* Assign state of directory. */
                        UniqueHash hashUnique;
                        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
                        itemsChained[index].hashUnique = hashUnique; /* Assign unique hash of specific path. */
                        itemsHash[hashAddress].indexHead = index; /* Finally fill the chained item into hash table. */
                        result = true;  /* Succeed. */
                    }
                }
            } else {                    /* If there is already a chain here. */
                UniqueHash hashUnique;
                getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
                uint64_t indexCurrent = indexHead; /* Index of current chained item. */
                uint64_t indexBeforeCurrent = 0; /* Index of item before current chained item. Initial value 0. */
                bool found = false;
                do {                    /* Traverse every chained item. */
                    if (memcmp(&(itemsChained[indexCurrent].hashUnique), &hashUnique, sizeof(UniqueHash)) == 0) {
                        itemsChained[indexCurrent].indexMeta = indexMeta; /* Update meta index. */
                        itemsChained[indexCurrent].isDirectory = isDirectory; /* Update state of directory. */
                        found = true;   /* Found one matched. */
                        break;          /* Jump out. */
                    } else {
                        indexBeforeCurrent = indexCurrent; /* Must be assigned at least once. */
                        indexCurrent = itemsChained[indexCurrent].indexNext; /* Move to next chained item. */
                    }
                } while (indexCurrent != 0); /* If current item is over last chained item then jump out (indexBeforeCurrent will point to last chained item). */
                if (found == true) {    /* If chained item has been found and updated. */
                    result = true;      /* Succeed. Updated meta index. */
                } else {                /* If there is no matched chained item, a new one need to be created. */
                    uint64_t index;
                    // if (bitmapChainedItems->findFree(&index) == false) { /* Method of bitmap search. */
                    if (headFreeBit == NULL) { /* Method of free bit chain. */
                        result = false; /* Fail due to no free bit in bitmap. */
                    } else {
                        index = headFreeBit->position; /* Get free bit index. */
                        FreeBit *currentFreeBit = headFreeBit; /* Get current free bit. */
                        headFreeBit = (FreeBit *)(headFreeBit->nextFreeBit); /* Move current free bit out of free bit chain. */
                        free(currentFreeBit); /* Release current free bit as used. */
                        if (bitmapChainedItems->set(index) == false) { /* Occupy the position first. Need not to roll back. */ 
                            result = false; /* Fail due to bitmap set error. */
                        } else {
                            itemsChained[index].indexNext = 0; /* Next item does not exist. */
                            itemsChained[index].indexMeta = indexMeta; /* Assign specific meta index. */
                            itemsChained[index].isDirectory = isDirectory; /* Assign state of directory. */
                            UniqueHash hashUnique;
                            HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
                            itemsChained[index].hashUnique = hashUnique; /* Assign unique hash of specific path. */
                            itemsChained[indexBeforeCurrent].indexNext = index; /* Finally fill the chained item into current last chained item. */
                            result = true;  /* Succeed. Created chained item. */
                        }
                    }
                }
            }
        }
        mutexBitmapChainedItems.unlock(); /* Unlock hash table bitmap. */
        return result;                  /* Return specific result. */
    }
}

/* Put a chained item by hash. If item has already existed, old meta index will be replaced by the new one. 
   @param   hashUnique  Unique hash.
   @param   indexMeta   Meta index to put in. 
   @param   isDirectory Judge if item is directory.
   @return              If error occurs return false, otherwise return true. */
bool HashTable::put(UniqueHash *hashUnique, uint64_t indexMeta, bool isDirectory)
{
    if (hashUnique == NULL) {
        return false;                   /* Fail due to null unique hash. */
    } else {
        AddressHash hashAddress = HashTable::getAddressHash(hashUnique); /* Get address hash by unique hash. */
        // getAddressHash(path, strlen(path), &hashAddress); /* Get address hash. */
        bool result;
        mutexBitmapChainedItems.lock(); /* Lock hash table bitmap. */
        {
            uint64_t indexHead = itemsHash[hashAddress].indexHead;
            // Debug::debugItem("put hashAddress = %d, indexHead = %d", hashAddress, indexHead);
            if (indexHead == 0) {       /* If there is no item currently. */
                uint64_t index;
                // if (bitmapChainedItems->findFree(&index) == false) { /* Method of bitmap search.
                if (headFreeBit == NULL) { /* Method of free bit chain. */
                    result = false;     /* Fail due to no free bit in bitmap. */
                } else {
                    index = headFreeBit->position; /* Get free bit index. */
                    FreeBit *currentFreeBit = headFreeBit; /* Get current free bit. */
                    headFreeBit = (FreeBit *)(headFreeBit->nextFreeBit); /* Move current free bit out of free bit chain. */
                    free(currentFreeBit); /* Release current free bit as used. */
                    if (bitmapChainedItems->set(index) == false) { /* Occupy the position first. Need not to roll back. */ 
                        result = false; /* Fail due to bitmap set error. */
                    } else {
                        // Debug::debugItem("Index = %d", index);
                        itemsChained[index].indexNext = 0; /* Next item does not exist. */
                        itemsChained[index].indexMeta = indexMeta; /* Assign specific meta index. */
                        itemsChained[index].isDirectory = isDirectory; /* Assign state of directory. */
                        // UniqueHash hashUnique;
                        // getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
                        itemsChained[index].hashUnique = *hashUnique; /* Assign unique hash of specific path. */
                        itemsHash[hashAddress].indexHead = index; /* Finally fill the chained item into hash table. */
                        result = true;  /* Succeed. */
                    }
                }
            } else {                    /* If there is already a chain here. */
                // UniqueHash hashUnique;
                // getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
                uint64_t indexCurrent = indexHead; /* Index of current chained item. */
                uint64_t indexBeforeCurrent = 0; /* Index of item before current chained item. Initial value 0. */
                bool found = false;
                do {                    /* Traverse every chained item. */
                    if (memcmp(&(itemsChained[indexCurrent].hashUnique), hashUnique, sizeof(UniqueHash)) == 0) {
                        itemsChained[indexCurrent].indexMeta = indexMeta; /* Update meta index. */
                        itemsChained[indexCurrent].isDirectory = isDirectory; /* Update state of directory. */
                        found = true;   /* Found one matched. */
                        break;          /* Jump out. */
                    } else {
                        indexBeforeCurrent = indexCurrent; /* Must be assigned at least once. */
                        indexCurrent = itemsChained[indexCurrent].indexNext; /* Move to next chained item. */
                    }
                } while (indexCurrent != 0); /* If current item is over last chained item then jump out (indexBeforeCurrent will point to last chained item). */
                if (found == true) {    /* If chained item has been found and updated. */
                    result = true;      /* Succeed. Updated meta index. */
                } else {                /* If there is no matched chained item, a new one need to be created. */
                    uint64_t index;
                    // if (bitmapChainedItems->findFree(&index) == false) { /* Method of bitmap search. */
                    if (headFreeBit == NULL) { /* Method of free bit chain. */
                        result = false; /* Fail due to no free bit in bitmap. */
                    } else {
                        index = headFreeBit->position; /* Get free bit index. */
                        FreeBit *currentFreeBit = headFreeBit; /* Get current free bit. */
                        headFreeBit = (FreeBit *)(headFreeBit->nextFreeBit); /* Move current free bit out of free bit chain. */
                        free(currentFreeBit); /* Release current free bit as used. */
                        if (bitmapChainedItems->set(index) == false) { /* Occupy the position first. Need not to roll back. */ 
                            result = false; /* Fail due to bitmap set error. */
                        } else {
                            itemsChained[index].indexNext = 0; /* Next item does not exist. */
                            itemsChained[index].indexMeta = indexMeta; /* Assign specific meta index. */
                            itemsChained[index].isDirectory = isDirectory; /* Assign state of directory. */
                            // UniqueHash hashUnique;
                            // getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
                            itemsChained[index].hashUnique = *hashUnique; /* Assign unique hash of specific path. */
                            itemsChained[indexBeforeCurrent].indexNext = index; /* Finally fill the chained item into current last chained item. */
                            result = true;  /* Succeed. Created chained item. */
                        }
                    }
                }
            }
        }
        mutexBitmapChainedItems.unlock(); /* Unlock hash table bitmap. */
        return result;                  /* Return specific result. */
    }
}

/* Delete a hash item.
   @param   path    Path.
   @return          If error occurs return false, otherwise return true. */
bool HashTable::del(const char *path)
{
    if (path == NULL) {
        return false;                   /* Fail due to null path. */
    } else {
        AddressHash hashAddress;
        HashTable::getAddressHash(path, strlen(path), &hashAddress); /* Get address hash. */
        bool result;
        mutexBitmapChainedItems.lock(); /* Lock hash table bitmap. */
        {
            uint64_t indexHead = itemsHash[hashAddress].indexHead;
            if (indexHead == 0) {
                result = false;         /* Fail due to no hash item. */
            } else {
                UniqueHash hashUnique;
                HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
                if (memcmp(&(itemsChained[indexHead].hashUnique), &hashUnique, sizeof(UniqueHash)) == 0) { /* If head chained item is matched. */
                    if (bitmapChainedItems->clear(indexHead) == false) {
                        result = false; /* Fail due to bitmap clear error. */
                    } else {            /* Remove chained item. Currently do not clear data of chained item. Data will be renew in put operation. */
                        FreeBit *currentFreeBit = (FreeBit *)malloc(sizeof(FreeBit)); /* New free bit. */
                        currentFreeBit->nextFreeBit = headFreeBit; /* Add current free bit to free bit chain. */
                        currentFreeBit->position = indexHead; /* Head index of chained items. */
                        headFreeBit = currentFreeBit; /* Update head free bit. */
                        itemsHash[hashAddress].indexHead = itemsChained[indexHead].indexNext; /* Might be 0 if head is the last. */
                        result = true;  /* Succeed. Removed head chained item. */
                    }
                } else {                /* If not head chained item. */
                    uint64_t indexBeforeCurrent = indexHead;
                    uint64_t indexCurrent = itemsChained[indexHead].indexNext;
                    bool found = false;
                    while (indexCurrent != 0) { /* indexCurrent might be 0 because head chained index has been examined. */
                        if (memcmp(&(itemsChained[indexCurrent].hashUnique), &hashUnique, sizeof(UniqueHash)) == 0) {
                            found = true;
                            break;
                        } else {
                            indexBeforeCurrent = indexCurrent; /* Assign indexBeforeCurrent. */
                            indexCurrent = itemsChained[indexCurrent].indexNext; /* Move to next chained item. */
                        }
                    }
                    if (found == false) {
                        result = true;  /* Succeed. No matched item is found. Here indexCurrent equals 0. */
                    } else {
                        if (bitmapChainedItems->clear(indexCurrent) == false) {
                            result = false; /* Fail due to bitmap clear error. */
                        } else {
                            FreeBit *currentFreeBit = (FreeBit *)malloc(sizeof(FreeBit)); /* New free bit. */
                            currentFreeBit->nextFreeBit = headFreeBit; /* Add current free bit to free bit chain. */
                            currentFreeBit->position = indexCurrent; /* Current index of chained items. */
                            headFreeBit = currentFreeBit; /* Update head free bit. */
                            itemsChained[indexBeforeCurrent].indexNext = itemsChained[indexCurrent].indexNext;
                            result = true; /* Succeed. Removed the chained item. */
                        }
                    }
                }
            }
        }
        mutexBitmapChainedItems.unlock(); /* Unlock hash table bitmap. */
        return result;                  /* Return specific result. */
    }
}

/* Delete a hash item by hash.
   @param   hashUnique      Unique hash.
   @return                  If error occurs return false, otherwise return true. */
bool HashTable::del(UniqueHash *hashUnique)
{
    if (hashUnique == NULL) {
        return false;                   /* Fail due to null unique hash. */
    } else {
        AddressHash hashAddress = HashTable::getAddressHash(hashUnique); /* Get address hash by unique hash. */
        // getAddressHash(path, strlen(path), &hashAddress); /* Get address hash. */
        bool result;
        mutexBitmapChainedItems.lock(); /* Lock hash table bitmap. */
        {
            uint64_t indexHead = itemsHash[hashAddress].indexHead;
            if (indexHead == 0) {
                result = false;         /* Fail due to no hash item. */
            } else {
                // UniqueHash hashUnique;
                // getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
                if (memcmp(&(itemsChained[indexHead].hashUnique), hashUnique, sizeof(UniqueHash)) == 0) { /* If head chained item is matched. */
                    if (bitmapChainedItems->clear(indexHead) == false) {
                        result = false; /* Fail due to bitmap clear error. */
                    } else {            /* Remove chained item. Currently do not clear data of chained item. Data will be renew in put operation. */
                        FreeBit *currentFreeBit = (FreeBit *)malloc(sizeof(FreeBit)); /* New free bit. */
                        currentFreeBit->nextFreeBit = headFreeBit; /* Add current free bit to free bit chain. */
                        currentFreeBit->position = indexHead; /* Head index of chained items. */
                        headFreeBit = currentFreeBit; /* Update head free bit. */
                        itemsHash[hashAddress].indexHead = itemsChained[indexHead].indexNext; /* Might be 0 if head is the last. */
                        result = true;  /* Succeed. Removed head chained item. */
                    }
                } else {                /* If not head chained item. */
                    uint64_t indexBeforeCurrent = indexHead;
                    uint64_t indexCurrent = itemsChained[indexHead].indexNext;
                    bool found = false;
                    while (indexCurrent != 0) { /* indexCurrent might be 0 because head chained index has been examined. */
                        if (memcmp(&(itemsChained[indexCurrent].hashUnique), hashUnique, sizeof(UniqueHash)) == 0) {
                            found = true;
                            break;
                        } else {
                            indexBeforeCurrent = indexCurrent; /* Assign indexBeforeCurrent. */
                            indexCurrent = itemsChained[indexCurrent].indexNext; /* Move to next chained item. */
                        }
                    }
                    if (found == false) {
                        result = true;  /* Succeed. No matched item is found. Here indexCurrent equals 0. */
                    } else {
                        if (bitmapChainedItems->clear(indexCurrent) == false) {
                            result = false; /* Fail due to bitmap clear error. */
                        } else {
                            FreeBit *currentFreeBit = (FreeBit *)malloc(sizeof(FreeBit)); /* New free bit. */
                            currentFreeBit->nextFreeBit = headFreeBit; /* Add current free bit to free bit chain. */
                            currentFreeBit->position = indexCurrent; /* Current index of chained items. */
                            headFreeBit = currentFreeBit; /* Update head free bit. */
                            itemsChained[indexBeforeCurrent].indexNext = itemsChained[indexCurrent].indexNext;
                            result = true; /* Succeed. Removed the chained item. */
                        }
                    }
                }
            }
        }
        mutexBitmapChainedItems.unlock(); /* Unlock hash table bitmap. */
        return result;                  /* Return specific result. */
    }
}

/* Get saved hash items count.
   @return      Return count of saved hash items. */
uint64_t HashTable::getSavedHashItemsCount()
{
    uint64_t count = 0;                 /* Initialize counter. */
    for (uint64_t i = 0; i < HASH_ITEMS_COUNT; i++) {
        if (itemsHash[i].indexHead != 0) { /* If hash item exists. */
            count++;                    /* Increment. */
        }
    }
    return count;                       /* Return count of saved hash items. */
}

/* Get saved chained items count.
   @return      Return count of saved chained items. */
uint64_t HashTable::getSavedChainedItemsCount()
{
    return (bitmapChainedItems->countTotal() - bitmapChainedItems->countFree() - 1); /* Return count of saved chained items. Reserved chained item 0 is set so there is an -1. */
}

/* Get total hash items count.
   @return      Return count of total hash items. */
uint64_t HashTable::getTotalHashItemsCount()
{
    return HASH_ITEMS_COUNT; /* Return count of total hash items. */
}

/* Get total chained items count.
   @return      Return count of total chained items. */
uint64_t HashTable::getTotalChainedItemsCount()
{
    return (bitmapChainedItems->countTotal() - 1); /* Return count of total chained items. The chained item 0 is reserved so there is an -1. */
}

/* Get max length of chain.
   @return      Return max length of chain. */
uint64_t HashTable::getMaxLengthOfChain()
{
    uint64_t max = 0, length;
    for (uint64_t i = 0; i < HASH_ITEMS_COUNT; i++) { /* Traverse all hash items. */
        length = 0;
        if (itemsHash[i].indexHead != 0) { /* If there is at least one chained item. */
            uint64_t indexCurrent = itemsHash[i].indexHead; /* Initialize current index to head. */
            do {
                length++;               /* First chained item exists. */
                indexCurrent = itemsChained[indexCurrent].indexNext; /* Move to next. */
            } while (indexCurrent != 0);
            if (length > max)
                max = length;           /* Assign current max length. */
        }
    }
    return max;                         /* Return max length of chain. */
}

/* Constructor of hash table. 
   @param   buffer          Buffer of whole table.
   @param   count           Max count of chained items. Can be divided by 8. No check here. */
HashTable::HashTable(char *buffer, uint64_t count)
{
    if (buffer == NULL) {
        fprintf(stderr, "HashTable: buffer is null.\n");
        exit(EXIT_FAILURE);
    } else {
        itemsHash = (HashItem *)buffer; /* Hash items pointer. */
        bitmapChainedItems = new Bitmap( /* Bitmap for chained items. */
            count, buffer + HASH_ITEMS_SIZE /* Need to be divided by 8 in bitmap class. */
        );
        itemsChained = (ChainedItem *)(buffer + HASH_ITEMS_SIZE + count / 8); /* Chained items pointer. Size of bitmap is count / 8. */
        if (bitmapChainedItems->set(0) == false) { /* Disable position 0 in bitmap due to reserved use. */
            fprintf(stderr, "HashTable: bitmap set error.\n");
            exit(EXIT_FAILURE);
        }
        sizeBufferUsed = HASH_ITEMS_SIZE + count / 8 + sizeof(ChainedItem) * count; /* Calculate size of used bytes in buffer. */
        // Debug::debugItem("itemsChainedAddress = %lx, sizeBufferUsed = %x", itemsChained, sizeBufferUsed);
        headFreeBit = NULL;
        FreeBit *currentFreeBit;
        for (uint64_t i = 1; i < bitmapChainedItems->countTotal(); i++) { /* Traverse all chained items to initialize free bits. Start from 1. */
            currentFreeBit = (FreeBit *)malloc(sizeof(FreeBit));
            currentFreeBit->position = i; /* Assign position. */
            currentFreeBit->nextFreeBit = headFreeBit; /* Push current free bit before head. */
            headFreeBit = currentFreeBit; /* Update head free bit. */
        }
    }
}

/* Destructor of hash table. */
HashTable::~HashTable()
{
    FreeBit *currentFreeBit;
    while (headFreeBit != NULL) {
        currentFreeBit = headFreeBit;
        headFreeBit = (FreeBit *)(headFreeBit->nextFreeBit); /* Move to next. */
        free(currentFreeBit);           /* Release free bit. */
    }
    delete bitmapChainedItems;          /* Release hash table bitmap. */
}
