package cn.xpleaf.common.algorithm._7sort;

import java.util.Arrays;

public class _2SelectSort {

    public static void main(String[] args) {
        int[] arr = {8, -2, 3, 9, 0, 1, 7, 6};
        System.out.println("排序前：" + Arrays.toString(arr));
        selectSort(arr);
        System.out.println("排序后：" + Arrays.toString(arr));
    }

    /**
     * 选择排序：
     *    每次从数组中找到一个最小的元素放到前面
     *    或者从i位置开始往后找，找一个最小的元素和i位置元组进行交换
     *       src:{8, -2, 3, 9, 0, 1, 7, 6} 假如数组长度为n
     *    第一趟：比较n-1
     *      {[-2], [8], 3, 9, 0, 1, 7, 6}
     *    第二趟：比较n-2
     *      {-2, [0], [8], 9, [3], 1, 7, 6}
     *    第三趟：比较n-3
     *      {-2, [0], [1], 9, [8], [3], 7, 6}
     *    第四趟：比较n-4
     *      {-2, 0, 1, [3], [9], [8], 7, 6}
     *    第五趟：比较n-5
     *      {-2, 0, 1, 3, [6], [9], [8], [7]}
     *    第六躺：比较n-6
     *      {-2, 0, 1, 3, 6, [7], [9], [8]}
     *    第七趟：比较n-7
     *      {-2, 0, 1, 3, 6, 7, 8, 9}
     *    比较的次数(n-1) + (n-2) + ... + 2 + 1=(n-1 + 1) * (n-1)/2 = n*(n-1)/2 = n^2/2 - n/2
     *    时间复杂O(n^2)
     *
     * @param arr
     */
    public static void selectSort(int[] arr) {
        for (int i = 0; i < arr.length - 1; i++) {
            for (int j = i + 1; j < arr.length; j++) {
                if(arr[i] > arr[j]) {//交换
                    swap(arr, i, j);
                }
            }
        }
    }

    private static void swap(int[] arr, int i, int j) {
        /*int tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;*/
        arr[i] = arr[i] ^ arr[j];
        arr[j] = arr[i] ^ arr[j];
        arr[i] = arr[i] ^ arr[j];
    }

}