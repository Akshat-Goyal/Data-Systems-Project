#include <math.h>

#include <iostream>
int main(int argv, char* argc[]) {
    int size = atoi(argc[1]);
    int num = 0;
    for (int i = 1; i <= size; i++) {
        for (int j = 1; j <= size; j++) {
            std::cout << num;
            if (j < size)
                std::cout << ", ";
            else
                std::cout << "\n";
            num++;
        }
    }
}
