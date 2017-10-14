/*** Storage classes. ***/

/** Version 2 + functional model modifications. **/

/** Included files. **/
#include <storage.hpp>

/** Implemented functions. **/
/* Get node hash of specific string. No check of length of buffer here.
   @param   buffer      Buffer of original data.
   @param   length      Length of buffer.
   @param   hashNode    Buffer of node hash. */
// NodeHash Storage::getNodeHash(const char *buffer)
// {
// //    if (buffer == NULL) {
// //        fprintf(stderr, "Storage::getNodeHash: buffer is null.\n");
// //        exit(EXIT_FAILURE);             /* Exit due to null buffer. */
// //    } else {
// //        int pos = 0, result = 0;
// //        while (buffer[pos] != '\0')     /* If not end. */
// //            result += buffer[pos++];    /* Add in iteration and move to next position. That is a simple hash. */
// //        result = result % countNode + 1; /* From 1 to countNode. Even if buffer is "" there is a return value (1). */
// //        return result;
// //    }
//      if (buffer == NULL) {
//         fprintf(stderr, "Storage::getNodeHash: buffer is null.\n");
//         exit(EXIT_FAILURE);             /* Exit due to null buffer. */
//      } else {
//         //uint64_t sha256_buffer[4];
//         //SHA256_CTX context;             /* Context. */
//         //sha256_init(&context);          /* Initialize SHA-256 context. */
//         //sha256_update(&context, (const BYTE *)buffer, strlen(buffer)); /* Update SHA-256 context. */
//         //sha256_final(&context, (BYTE *)sha256_buffer); /* Finalize SHA-256 context. */
//         return 1;
//   	//return ((sha256_buffer[3] % countNode) + 1); /* Return node hash. From 1 to countNode. */
//     }
// }

/* Get node hash of unique hash.
   @param   uniqueHash  Unique hash.
   @param   length      Length of buffer.
   @param   hashNode    Buffer of node hash. */
NodeHash Storage::getNodeHash(UniqueHash *hashUnique)
{
    if (hashUnique == NULL) {
        fprintf(stderr, "Storage::getNodeHash: buffer is null.\n");
        exit(EXIT_FAILURE);             /* Exit due to null unique hash. */
    } else {
        return ((hashUnique->value[3] % countNode) + 1); /* Return node hash. From 1 to countNode. */
    }
}

/* Constructor. Mainly allocate memory.
   @param   countFile       Max count of files.
   @param   countDirectory  Max count of directories.
   @param   countBlock      Max count of blocks.
   @param   countNode       Count of nodes. */
Storage::Storage(char *buffer, char* bufferBlock, uint64_t countFile, uint64_t countDirectory, uint64_t countBlock, uint64_t countNode)
{
    if ((buffer == NULL) || (bufferBlock == NULL) || (countFile == 0) || (countDirectory == 0) ||
        (countBlock == 0) || (countNode == 0)) {
        fprintf(stderr, "Storage::Storage: parameter error.\n");
        exit(EXIT_FAILURE);             /* Exit due to parameter error. */
    } else {
        hashtable = new HashTable(buffer, countDirectory + countFile); /* Initialize hash table. */
        Debug::notifyInfo("sizeof Hash Table = %d MB", hashtable->sizeBufferUsed / 1024 / 1024);

        tableFileMeta = new Table<FileMeta>(buffer + hashtable->sizeBufferUsed, countFile); /* Initialize file meta table. */
        Debug::notifyInfo("sizeof File Meta Size = %d MB", tableFileMeta->sizeBufferUsed / 1024 / 1024);

        tableDirectoryMeta = new Table<DirectoryMeta>(buffer + hashtable->sizeBufferUsed + tableFileMeta->sizeBufferUsed, countDirectory); /* Initialize directory meta table. */
        Debug::notifyInfo("sizeof Directory Meta Size = %d MB", tableDirectoryMeta->sizeBufferUsed / 1024 / 1024);

        tableBlock = new Table<Block>(bufferBlock, countBlock); /* Initialize block table. */
        
        this->countNode = countNode;    /* Assign count of nodes. */
        sizeBufferUsed = hashtable->sizeBufferUsed + tableFileMeta->sizeBufferUsed + tableDirectoryMeta->sizeBufferUsed + tableBlock->sizeBufferUsed; /* Size of used bytes in buffer. */
    }
}

/* Deconstructor. Mainly free memory. */
Storage::~Storage()
{
    delete hashtable;                   /* Release memory for hash table. */
    delete tableFileMeta;               /* Release memory for file meta table. */
    delete tableDirectoryMeta;          /* Release memory for directory meta table. */
    delete tableBlock;                  /* Release memory for block table. */
}
