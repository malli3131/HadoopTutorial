#include<stdio.h>
#include<stdlib.h>

int* doubleArray(int* a, int n)
{
	int *myArray = malloc(n*sizeof(int));
	int i = 0;
	for(i=0; i<n; i++)
	{
		*(myArray+i) = 2 * (*(a+i));
	}
	return myArray;
}
int main()
{
	int a[] = {2, 4, 6, 8, 10};
	int size = sizeof(a) / sizeof(int);
	int* p = doubleArray(a, size);
	int k = 0;
	for(k = 0; k < size; k++)
	{
		printf("%d\t", *(p+k));
	}
	printf("\n");
	return 0;
}
