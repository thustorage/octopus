#include "org_apache_hadoop_fs_nrfs_RawFileSystem.h"
#include "nrfs.h"

JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_nrfs_RawFileSystem_nrfsConnect
  (JNIEnv *env, jobject obj)
  {
    return (jint)nrfsConnect("defalut", 0, 0);
  }


JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_nrfs_RawFileSystem_nrfsDisconnect
  (JNIEnv *env, jobject)
  {
    return (jint)nrfsDisconnect(0);
  }


JNIEXPORT jlong JNICALL Java_org_apache_hadoop_fs_nrfs_RawFileSystem_nrfsOpenFile
  (JNIEnv *env, jobject obj, jbyteArray buf, jint flags)
  {
    jbyte *_path = env->GetByteArrayElements(buf, 0);
    char *path = (char *)_path;
    nrfsFile file = nrfsOpenFile(0, path, (int)flags);
    return (jlong)file;
  }


JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_nrfs_RawFileSystem_nrfsCloseFile
  (JNIEnv *env, jobject obj, jbyteArray buf)
  {
    int ret;
    jbyte *_path = env->GetByteArrayElements(buf, 0);
    char *path = (char *)_path;
    ret = nrfsCloseFile(0, path);
    env->ReleaseByteArrayElements(buf, _path, 0);
    return (jint)ret;
  }


JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_nrfs_RawFileSystem_nrfsMknod
  (JNIEnv *env, jobject obj, jbyteArray buf)
  {
    int ret;
    jbyte *_path = env->GetByteArrayElements(buf, 0);
    char *path = (char *)_path;
    ret = nrfsMknod(0, path);
    env->ReleaseByteArrayElements(buf, _path, 0);
    return (jint)ret;
  }


JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_nrfs_RawFileSystem_nrfsAccess
  (JNIEnv *env, jobject obj, jbyteArray buf)
  {
    int ret;
    jbyte *_path = env->GetByteArrayElements(buf, 0);
    char *path = (char *)_path;
    ret = nrfsAccess(0, path);
    env->ReleaseByteArrayElements(buf, _path, 0);
    return (jint)ret;
  }


JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_nrfs_RawFileSystem_nrfsGetAttibute
  (JNIEnv *env, jobject obj, jbyteArray file, jintArray _properties)
  {
    FileMeta attr;
    int ret;
    jbyte *path = env->GetByteArrayElements(file, 0);
    jint *properties = env->GetIntArrayElements(_properties, 0);
    ret = nrfsGetAttribute(0, (char *)path, &attr);

    properties[0] = (jint)attr.size;
    /* isDirectory = 1 */
    properties[1] = (jint)((attr.count == MAX_FILE_EXTENT_COUNT) ? 1 : 0);
    
    env->ReleaseByteArrayElements(file, path, 0);
    env->ReleaseIntArrayElements(_properties, properties, 0);
    return (jint)ret;
  }


JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_nrfs_RawFileSystem_nrfsWrite
  (JNIEnv *env, jobject obj, jbyteArray _path, jbyteArray _buffer, jint size, jint offset)
  {
    int ret;
    jbyte *path = env->GetByteArrayElements(_path, 0);
    jbyte *buffer = env->GetByteArrayElements(_buffer, 0);
    // unsigned char *buffer = (unsigned char*)env->GetDirectBufferAddress(_buffer);
    ret = nrfsWrite(0, (char *)path, (void *)buffer, (uint64_t)size, (uint64_t)offset);
    env->ReleaseByteArrayElements(_path, path, 0);
    env->ReleaseByteArrayElements(_buffer, buffer, 0);
    return ret;
  }


JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_nrfs_RawFileSystem_nrfsRead
  (JNIEnv *env, jobject obj, jbyteArray _path, jbyteArray _buffer, jint size, jint offset)
  {
    int ret;
    jbyte *path = env->GetByteArrayElements(_path, 0);
    jbyte *buffer = env->GetByteArrayElements(_buffer, 0);
    ret = nrfsRead(0, (char *)path, (void *)buffer, (uint64_t)size, (uint64_t)offset);
    env->ReleaseByteArrayElements(_path, path, 0);
    env->ReleaseByteArrayElements(_buffer, buffer, 0);
    return ret;
  }


JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_nrfs_RawFileSystem_nrfsCreateDirectory
  (JNIEnv *env, jobject obj, jbyteArray _path)
  {
    int ret;
    jbyte *path = env->GetByteArrayElements(_path, 0);
    ret =nrfsCreateDirectory(0, (char *)path);
    env->ReleaseByteArrayElements(_path, path, 0);
    return (jint)ret;
  }


JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_nrfs_RawFileSystem_nrfsDelete
  (JNIEnv *env, jobject obj, jbyteArray _path)
  {
    int ret;
    jbyte *path = env->GetByteArrayElements(_path, 0);
    ret = nrfsDelete(0, (char *)path);
    env->ReleaseByteArrayElements(_path, path, 0);
    return (jint)ret;
  }


JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_nrfs_RawFileSystem_nrfsRename
  (JNIEnv *env, jobject obj, jbyteArray _oldPath, jbyteArray _newPath)
  {
    int ret;
    jbyte *oldPath = env->GetByteArrayElements(_oldPath, 0);
    jbyte *newPath = env->GetByteArrayElements(_newPath, 0);
    ret = nrfsRename(0, (char *)oldPath, (char *)newPath);
    env->ReleaseByteArrayElements(_oldPath, oldPath, 0);
    env->ReleaseByteArrayElements(_newPath, newPath, 0);
    return (jint)ret;
  }


JNIEXPORT jint JNICALL Java_org_apache_hadoop_fs_nrfs_RawFileSystem_nrfsListDirectory
  (JNIEnv *env, jobject obj, jbyteArray _path, jbyteArray _properties)
  {
    int ret, i;
    nrfsfilelist list;
    jbyte *path = env->GetByteArrayElements(_path, 0);
    jbyte *properties = env->GetByteArrayElements(_properties, 0);
    ret = nrfsListDirectory(0, (char *)path, &list);

    ret = list.count;
    for(i = 0; i < ret; i++)
      memcpy((void*)((char*)properties + sizeof(char) * MAX_FILE_NAME_LENGTH * i), list.tuple[i].names, sizeof(char) * MAX_FILE_NAME_LENGTH);

    env->ReleaseByteArrayElements(_path, path, 0);
    env->ReleaseByteArrayElements(_properties, properties, 0);
    return ret;
  }
