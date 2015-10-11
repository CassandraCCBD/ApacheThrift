#include<stdio.h>
#include<jni.h>
#include<syscall.h>
#include "org_apache_thrift_ProcessFunction.h"

JNIEXPORT jint JNICALL Java_org_apache_thrift_ProcessFunction_HelloWorld(JNIEnv * env, jobject obj)
{
	FILE *fptr;
	/*fptr=fopen("/home/yasu/Summer\ Stuff/JNI/try", "w");
	fprintf(fptr, "in Hello");*/
	jint tid = syscall(__NR_gettid);
	return tid;
}
