package cn.xpleaf.common.algorithm._7sort;

import java.util.Arrays;

public class _4BubbleSort {

    public static void main(String[] args) {
        int[] arr = {8, -2, 3, 9, 0, 1, 7, 6};
        System.out.println("排序前：" + Arrays.toString(arr));
        bubbleSort(arr);
        System.out.println("排序后：" + Arrays.toString(arr));
    }

    /**
     * 排序过程分析：
     *                                     i前       比较次数（arr.length为8）
     *     开始：8, -2, 3, 9, 0, 1, 7, 6
     *
     * 第1趟结束：-2, 3, 8, 0, 1, 7, 6, 9    0           7
     *
     * 第2趟结束：-2, 3, 0, 1, 7, 6, 8, 9    1           6
     *
     * 第3趟结束：-2, 0, 1, 3, 6, 7, 8, 9    2           5
     *
     * 第4趟结束：-2, 0, 1, 3, 6, 7, 8, 9    3           4
     *
     * 第5趟结束：-2, 0, 1, 3, 6, 7, 8, 9    4           3
     *
     * 第6趟结束：-2, 0, 1, 3, 6, 7, 8, 9    5           2
     *
     * 第7趟结束：-2, 0, 1, 3, 6, 7, 8, 9    6           1
     *
     * 结论：需要比较的趟数为 arr.length - 1
     *      每一趟需要比较   arr.length - 1 - i 次
     *
     * 由于冒泡排序为相邻两者相互比较对调，并不会更改其原本排序的顺序，所以是稳定排序法
     */
    public static void bubbleSort(int[] arr) {
        /**
         * 外层i只是控制比较的趟数，假设元素个数为n，那么比较的趟数就为n-1
         */
        for(int i = 0; i < arr.length - 1; i++) {
            for(int j = 0; j < arr.length - 1 - i; j++) {
                if(arr[j] > arr[j + 1]) {
                    swap(arr, j, j + 1);
                }
            }
        }
    }

    public static void swap(int[] arr, int i, int j) {
        arr[i] = arr[i] ^ arr[j];
        arr[j] = arr[i] ^ arr[j];
        arr[i] = arr[i] ^ arr[j];
    }
}