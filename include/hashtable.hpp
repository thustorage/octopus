/*** Hash table header for index in file system. ***/

/** Version 2 + Crypto++ support. **/

/** Redundance check. **/
#ifndef HASHTABLE_HEADER
#define HASHTABLE_HEADER

/** Included files. **/
#include <stdint.h>                     /* Standard integers. E.g. uint16_t */
#include <assert.h>                     /* Assert function. E.g. assert() */
#include <stdlib.h>                     /* Standard library. E.g. exit() */
#include <string.h>                     /* String operations. E.g. memcmp() */
#include <stdio.h>                      /* Standard I/O. */
#include <mutex>                        /* Mutex operations. */
#include "bitmap.hpp"                   /* Bitmap class. */
#include "debug.hpp"                    /* Debug class. */
// #include "sha256.h"                     /* SHA-256 algorithm. */
#include "table.hpp"                    /* Table template. Use FreeBit structure. */
#include <cryptopp/sha.h>                        /* SHA-256 algorithm in Crypto++ library. */

/** Name space. **/
using namespace CryptoPP;

/** Design. **/

/*
    Chosen algorithm:
        UniqueHash: SHA-256
        AddressHash (describe in Verilog pseudocode): 256'b SHA-256[20:0]

                   -  Hash items  -                      -  Chained items -
                  (Fixed size: 16 MB)                  (Size depend on buffer)   (due to alignment)
   AddressHash  | 8 bytes  | 8 bytes       |   | 32 bytes           | 8 bytes    | 8 bytes     | 8 bytes      |
                +----------+---------------+   +--------------------+------------+-------------+--------------+            +--------------------+------------+----------------+
  0 * 16 bytes  |   Lock   |   Head index -|-->|  UniqueHash(Path)  | Meta index | isDirectory | Next index  -|--> .... -->|  UniqueHash(Path)  | Meta Index |        0       |
                +----------+---------------+   +--------------------+------------+-------------+--------------+            +--------------------+------------+----------------+
  1 * 16 bytes  |   Lock   |   Head index -|-->|                    |            |             |             -|--> ....                                      0 for end of chain
                +----------+---------------+   +--------------------+------------+-------------+--------------+
                |          |               |   |                       An item of chain                       |
                |   ....   |      ....     |   
                |          |               |
                +----------+---------------+
 1M * 16 bytes  |   Lock   |   Head index -|--> ....
                +----------+---------------+
                      Here link and next are both index of chained items array

                    +---------------------+--------------------------+-----------------------------------------+
  - Total buffer -  |      Hash items     |  Bitmap of chained items |                Chained items            |
                    +---------------------+--------------------------+-----------------------------------------+
                    |  16 MB              |   (count / 8) bytes      |   (count * sizeof(ChainedItem)) bytes   |

    
    Position actually contains index of chained item.
    +--------------+-------------------------+             +--------------+----------+
    |   Position   |  Next free bit pointer -|--> .... --->|   Position   |   NULL   |
    +--------------+-------------------------+             +--------------+----------+
                                    - Free bit chain -
*/

/** Definitions. **/
#define HASH_ADDRESS_BITS 20            /* 20 bits can hold 1048576 hash items. */
#define HASH_ITEMS_COUNT (1 << HASH_ADDRESS_BITS) /* Actual count of hash items. */
#define HASH_ITEMS_SIZE (HASH_ITEMS_COUNT * sizeof(HashItem)) /* Size of hash items in bytes. */

/** Structures. **/
typedef uint64_t AddressHash;           /* Address hash definition for locate. */
                                        /* 8-byte address is far enough. Acutally 20-bit (2.5-byte) is enough, but an 64-bit variable is better in address computation. */

typedef struct {                        /* Unique hash structure for identify a unique path. There might be collision. */
  //char bytes[32];                       /* 32-byte variable can hold all data in SHA-256 format. */
  uint64_t value[4];
} UniqueHash;

typedef struct {                        /* Hash item structure. Total 16 bytes. Currently container class and type of item are combined together. */
    uint64_t key;                       /* Key for lock. */
    uint64_t indexHead;                 /* Index of head chained item. Value 0 is reserved for no item. */
} HashItem;

typedef struct {                        /* Chained item structure. Total 32 bytes. */
    uint64_t indexNext;                 /* Index of next chained item. Value 0 is reserved for no item. */
    uint64_t indexMeta;                 /* Index of meta. */
    bool isDirectory;                   /* Mark directory if true. Mark file if false. Align to 8 bytes. */
    UniqueHash hashUnique;              /* 32-byte unique hash for path. Lack of completeness. */
} ChainedItem;

/* Use version in table template. */
// typedef struct {                        /* Free bit structure. */
//     uint64_t position;                  /* Position of bit. */
//     void *nextFreeBit;                  /* Next free bit. */
// } FreeBit;

/** Classes. **/
class HashTable
{
private:
    Bitmap *bitmapChainedItems;         /* Bitmap for chained items. */
    std::mutex mutexBitmapChainedItems; /* Mutex for bitmap for chained items. */
    HashItem *itemsHash;                /* Hash items of hash table. */
    ChainedItem *itemsChained;          /* Chained items of hash table. */
    FreeBit *headFreeBit;               /* Head free bit in the chain. */
    
public:
    static void getAddressHash(const char *buf, uint64_t len, AddressHash *hashAddress); /* Get address hash of specific string. */
    static AddressHash getAddressHash(UniqueHash *hashUnique); /* Get address hash by unique hash. */
    static void getUniqueHash(const char *buf, uint64_t len, UniqueHash *hashUnique); /* Get unique hash of specific string. */
    uint64_t sizeBufferUsed;            /* Size of used bytes in buffer. */
    bool get(const char *path, uint64_t *indexMeta, bool *isDirectory); /* Get a chained item. */
    bool get(UniqueHash *hashUnique, uint64_t *indexMeta, bool *isDirectory); /* Get a chained item by hash. */
    bool put(const char *path, uint64_t indexMeta, bool isDirectory); /* Put a chained item. */
    bool put(UniqueHash *hashUnique, uint64_t indexMeta, bool isDirectory); /* Put a chained item by hash. */
    bool del(const char *path);         /* Delete a hash item. */
    bool del(UniqueHash *hashUnique);   /* Delete a hash item by hash. */
    uint64_t getSavedHashItemsCount();  /* Get saved hash items count. */
    uint64_t getSavedChainedItemsCount(); /* Get saved chained items count. */
    uint64_t getTotalHashItemsCount();  /* Get total hash items count. */
    uint64_t getTotalChainedItemsCount(); /* Get total chained items count. */
    uint64_t getMaxLengthOfChain();     /* Get max length of chain. */
    HashTable(char *buffer, uint64_t count); /* Constructor of hash table. */
    ~HashTable();                       /* Destructor of hash table. */
};

/** Redundance check. **/
#endif
