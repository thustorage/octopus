/*** Table header in file system. ***/

/** Version 1. **/

/* FIXME: Ignore malloc() failure. */

/** Redundance check. **/
#ifndef TABLE_HEADER
#define TABLE_HEADER

/** Included files. **/
#include <stdint.h>                     /* Standard integers. E.g. uint16_t */
#include <stdlib.h>                     /* Standard library. E.g. exit() */
#include <string.h>                     /* String operations. E.g. memcmp() */
#include <stdio.h>                      /* Standard I/O. */
#include <mutex>                        /* Mutex operations. */
#include "bitmap.hpp"                   /* Bitmap class. */
#include "debug.hpp"                    /* Debug class. */

/** Design. **/

/*
    +------------+---------------------------+
    |   Bitmap   |           Items           |
    +------------+---------------------------+
          - Memory overview of buffer -


    Use bitmap to check if a position is occupied.
    +---+---+---+---+---+---+------+---+---+---+---+
    | 0 | 0 | 0 | 1 | 0 | 1 | .... | 0 | 0 | 0 | 0 |
    +---+---+---+---+---+---+------+---+---+---+---+
                - Structure of bitmap -
    
    Use free bit chain to allocate a new item. (The free bit chain is generated from bitmap.)
    +--------------+-------------------------+             +--------------+----------+
    |   Position   |  Next free bit pointer -|--> .... --->|   Position   |   NULL   |
    +--------------+-------------------------+             +--------------+----------+
                                    - Free bit chain -
                      index
                            +--------------+
                        0   |    Item 0    |
                            +--------------+
                        1   |    Item 1    |
                            +--------------+
      - item array -    2   |    Item 2    |
                            +--------------+
                            |     ....     |
                            +--------------+
                        n   |    Item n    |
                            +--------------+

*/

/** Structures. **/
typedef struct {                        /* Free bit structure. */
    uint64_t position;                  /* Position of bit. */
    void *nextFreeBit;                  /* Next free bit. */
} FreeBit;

/** Classes. **/
template<typename T> class Table
{
private:
    Bitmap *bitmapItems;                /* Bitmap for items. */
    std::mutex mutexBitmapItems;        /* Mutex for bitmap for items. */
    T *items;                           /* Items of table. */
    FreeBit *headFreeBit;               /* Head free bit in the chain. */
    FreeBit *tailFreeBit;               /* Tail free bit in the chain. */

public:
    uint64_t sizeBufferUsed;            /* Size of used bytes in buffer. */
    bool create(uint64_t *index, T *item); /* Create an item. */
    bool create(uint64_t *index);       /* Create an item. No item will be assigned. */
    bool get(uint64_t index, T *item);  /* Get an item. */
    bool get(uint64_t index, T *item, uint64_t *address);
    bool put(uint64_t index, T *item);  /* Put an item. */
    bool put(uint64_t index, T *item, uint64_t *address);
    bool remove(uint64_t index);        /* Remove an item. */
    uint64_t countSavedItems();         /* Saved items count. */
    uint64_t countTotalItems();         /* Total items count. */
    Table(char *buffer, uint64_t count); /* Constructor of table. */
    ~Table();                           /* Destructor of table. */
};

/* Create an item in table. (no item will be assigned)
   @param   index   Index of created item in table.
   @return          If creation failed return false. Otherwise return true. */
template<typename T> bool Table<T>::create(uint64_t *index)
{
    if (index == NULL) {
        return false;                   /* Fail due to null index. */
    } else {
        bool result;
        mutexBitmapItems.lock();        /* Lock table bitmap. */
        {
            if (headFreeBit == NULL) {  /* If there is no free bit in bitmap. */
                return false;           /* Fail due to out of free bit. */
            } else {
                *index = headFreeBit->position; /* Get index from free bit. */
                FreeBit *currentFreeBit = headFreeBit; /* Get current free bit. */
                headFreeBit = (FreeBit *)(headFreeBit->nextFreeBit); /* Move current free bit out of free bit chain. */
                free(currentFreeBit);   /* Release current free bit as used. */
                if (bitmapItems->set(*index) == false) { /* Occupy the position first. Need not to roll back. */ 
                    result = false;     /* Fail due to bitmap set error. No recovery here. */
                } else {
                    result = true;      /* Succeed. */
                }
            }
        }
        mutexBitmapItems.unlock();      /* Unlock table bitmap. */
        //printf("Table::create (block): *index = %lu, &(items[*index]) = %lx\n", *index, &(items[*index]));
        return result;                  /* Return specific result. */
    }
}

/* Create an item in table.
   @param   index   Index of created item in table.
   @param   item    Item to put in table. 
   @return          If creation failed return false. Otherwise return true. */
template<typename T> bool Table<T>::create(uint64_t *index, T *item)
{
    if ((index == NULL) || (item == NULL)) {
        return false;                   /* Fail due to null index or null item. */
    } else {
        bool result;
        mutexBitmapItems.lock();        /* Lock table bitmap. */
        {
            if (headFreeBit == NULL) {  /* If there is no free bit in bitmap. */
                return false;           /* Fail due to out of free bit. */
            } else {
                *index = headFreeBit->position; /* Get index from free bit. */
                FreeBit *currentFreeBit = headFreeBit; /* Get current free bit. */
                headFreeBit = (FreeBit *)(headFreeBit->nextFreeBit); /* Move current free bit out of free bit chain. */
                free(currentFreeBit);   /* Release current free bit as used. */
                if (bitmapItems->set(*index) == false) { /* Occupy the position first. Need not to roll back. */ 
                    result = false;     /* Fail due to bitmap set error. No recovery here. */
                } else {
                    items[*index] = *item; /* Save item in items. */
                    result = true;      /* Succeed. */
                }
            }
        }
        mutexBitmapItems.unlock();      /* Unlock table bitmap. */
        return result;                  /* Return specific result. */
    }
}


/* Get an item from table.
   @param   index   Index of item.
   @param   item    Buffer of item. 
   @return          If item does not exist or other errors occur return false. Otherwise return true. */
template<typename T> bool Table<T>::get(uint64_t index, T *item)
{
    //printf("Table::get: index = %lu\n", index);
    if (item == NULL) {
        return false;                   /* Fail due to null item buffer. */
    } else {
        bool result;
        mutexBitmapItems.lock();        /* Though currently there is no bitmap reading or writing, other operations such as delete might affect item reading. */
        {
            bool status;                /* Status of existence of item. */
            if (bitmapItems->get(index, &status) == false) {
                result = false;         /* Fail due to item get error. */
            } else {
                if (status == false) {
                    result = false;     /* Fail due to no item. */
                } else {
                    *item = items[index]; /* Get item and put it into buffer. */
                    result = true;      /* Succeed. */
                }
            }
        }
        mutexBitmapItems.unlock();      /* Unlock table bitmap. */
        return result;                  /* Return specific result. */
    }
}

template<typename T> bool Table<T>::get(uint64_t index, T *item, uint64_t *address)
{
    //printf("Table::get: index = %lu\n", index);
    if (item == NULL) {
        return false;                   /* Fail due to null item buffer. */
    } else {
        bool result;
        mutexBitmapItems.lock();        /* Though currently there is no bitmap reading or writing, other operations such as delete might affect item reading. */
        {
            bool status;                /* Status of existence of item. */
            if (bitmapItems->get(index, &status) == false) {
                result = false;         /* Fail due to item get error. */
            } else {
                if (status == false) {
                    result = false;     /* Fail due to no item. */
                } else {
                    *item = items[index]; /* Get item and put it into buffer. */
                    *address = (uint64_t)(&items[index]);
                    Debug::debugItem("get, address = %lx", *address);
                    result = true;      /* Succeed. */
                }
            }
        }
        mutexBitmapItems.unlock();      /* Unlock table bitmap. */
        return result;                  /* Return specific result. */
    }
}

/* Put an item. If item has not been created then return false. 
   @param   index   Index of item.
   @param   item    Item to put in.
   @return          If index has not been created or other errors occur return false, otherwise return true. */
template<typename T> bool Table<T>::put(uint64_t index, T *item)
{
    //printf("Table::put: index = %lu\n", index);
    if (item == NULL) {
        return false;                   /* Fail due to null item. */
    } else {
        bool result;
        mutexBitmapItems.lock();        /* Lock table bitmap. */
        {
            bool status;                /* Status of existence of item. */
            if (bitmapItems->get(index, &status) == false) {
                result = false;         /* Fail due to item get error. */
            } else {
                if (status == false) {
                    result = false;     /* Fail due to no item. */
                } else {
                    items[index] = *item; /* Save item into table. */
                    result = true;      /* Succeed. */
                }
            }
        }
        mutexBitmapItems.unlock();      /* Unlock table bitmap. */
        return result;                  /* Return specific result. */
    }
}

/* Get address of item. 
   @param   index   Index of item.
   @param   item    Item to put in.
   @return          If index has not been created or other errors occur return false, otherwise return true. */
template<typename T> bool Table<T>::put(uint64_t index, T *item, uint64_t *address)
{
    bool result;
    mutexBitmapItems.lock();        /* Lock table bitmap. */
    {
        bool status;                /* Status of existence of item. */
        if (bitmapItems->get(index, &status) == false) {
            result = false;         /* Fail due to item get error. */
        } else {
            if (status == false) {
                result = false;     /* Fail due to no item. */
            } else {
                *address = (uint64_t)&items[index]; /* Save item into table. */
                Debug::debugItem("put, address = %lx", *address);
                //items[index] = *item;
                result = true;      /* Succeed. */
            }
        }
    }
    mutexBitmapItems.unlock();      /* Unlock table bitmap. */
    return result;                  /* Return specific result. */
}

/* Remove an item.
   @param   index   Index of item to remove.
   @return          If error occurs return false, otherwise return true. */
template<typename T> bool Table<T>::remove(uint64_t index)
{
    bool result;
    //printf("Table::remove: index = %lu\n", index);
    mutexBitmapItems.lock();            /* Lock table bitmap. */
    {
        bool status;                    /* Status of existence of item. */
        if (bitmapItems->get(index, &status) == false) {
            result = false;             /* Fail due to bitmap get error. */
        } else {
            if (status == false) {
                result = false;         /* Fail due to no item. */
            } else {
                if (bitmapItems->clear(index) == false) {
                    result = false;     /* Fail due to bitmap clear error. */
                } else {
                    FreeBit *currentFreeBit = (FreeBit *)malloc(sizeof(FreeBit)); /* New free bit. */
                    currentFreeBit->nextFreeBit = headFreeBit; /* Add current free bit to free bit chain. */
                    currentFreeBit->position = index; /* Index of newly released item. */
                    headFreeBit = currentFreeBit; /* Update head free bit. */
                    result = true;      /* Succeed. */
                }
            }
        }
    }
    mutexBitmapItems.unlock();          /* Unlock table bitmap. */
    return result;                      /* Return specific result. */
}
 
/* Get saved items count.
   @return      Return count of saved items. */
template<typename T> uint64_t Table<T>::countSavedItems()
{
    return (bitmapItems->countTotal() - bitmapItems->countFree()); /* Return count of saved items. */
}

/* Get total items count.
   @return      Return count of total items. */
template<typename T> uint64_t Table<T>::countTotalItems()
{
    return (bitmapItems->countTotal()); /* Return count of total items. */
}

/* Constructor of table. Initialize bitmap, items array and free bit chain. 
   @param   buffer  Buffer of whole table (including bitmap and items).
   @param   count   Count of items in table (can be divided by 8 due to bitmap requirement). */
template<typename T> Table<T>::Table(char *buffer, uint64_t count)
{
    if (buffer == NULL) {
        fprintf(stderr, "Table: buffer is null.\n");
        exit(EXIT_FAILURE);             /* Fail due to null buffer pointer. */
    } else {
        if (count % 8 != 0) {
            fprintf(stderr, "Table: count should be times of eight.\n");
            exit(EXIT_FAILURE);         /* Fail due to count alignment. */
        } else {
            bitmapItems = new Bitmap(count, buffer + count * sizeof(T)); /* Initialize item bitmap. */
            items = (T *)(buffer); /* Initialize items array. */
            sizeBufferUsed = count / 8 + count * sizeof(T); /* Size of used bytes in buffer. */
            /*  
                Initialize free bit chain.
            */
            headFreeBit = NULL;         /* Initialize head free bit pointer. */
            FreeBit *currentFreeBit;
            // for (uint64_t index = 0; index < count / 8; index++) { /* Search in all bytes in array. */
            //     for (uint64_t offset = 0; offset <= 7; offset++) { /* Search in all offsets in byte. */
            //         if ((buffer[index] & (1 << (7 - offset))) == 0) { /* Judge if bit is cleared. */
            //             currentFreeBit = (FreeBit *)malloc(sizeof(FreeBit)); /* Create a new free bit. */
            //             currentFreeBit->position = index * 8 + offset; /* Assign position. */
            //             currentFreeBit->nextFreeBit = headFreeBit; /* Push current free bit before head. */
            //             headFreeBit = currentFreeBit; /* Update head free bit. */
            //         }
            //     }
            // }
            for (int index = count / 8 - 1; index >= 0; index--) { /* Search in all bytes in array. */
                for (int offset = 7; offset >= 0; offset--) { /* Search in all offsets in byte. */
                    if ((buffer[index] & (1 << (7 - offset))) == 0) { /* Judge if bit is cleared. */
                        currentFreeBit = (FreeBit *)malloc(sizeof(FreeBit)); /* Create a new free bit. */
                        currentFreeBit->position = index * 8 + offset; /* Assign position. */
                        currentFreeBit->nextFreeBit = headFreeBit; /* Push current free bit before head. */
                        headFreeBit = currentFreeBit; /* Update head free bit. */
                    }
                }
            }
        }
    }
}

/* Destructor of table. */
template<typename T> Table<T>::~Table()
{
    FreeBit *currentFreeBit;
    while (headFreeBit != NULL) {
        currentFreeBit = headFreeBit;
        headFreeBit = (FreeBit *)(headFreeBit->nextFreeBit); /* Move to next. */
        free(currentFreeBit);           /* Release free bit. */
    }
    delete bitmapItems;                 /* Release table bitmap. */
}

/** Redundance check. **/
#endif