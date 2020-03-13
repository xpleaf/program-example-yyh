package cn.xpleaf.common.algorithm._7sort;

import java.util.Arrays;

public class _3InsertSort {

    public static void main(String[] args) {
        int[] arr = {8, -2, 3, 9, 0, 1, 7, 6};
        System.out.println("排序前：" + Arrays.toString(arr));
        insertSort(arr);
        System.out.println("排序后：" + Arrays.toString(arr));
    }

    /**
     * 插入排序：
     *    试图将一个元素插入到一个有序集合的恰当的位置
     *    src: {8, -2, 3, 9, 0, 1, 7, 6}
     *    假设数组长度为n
     *    第一趟：比较次数 1
     *      {[-2], [8], 3, 9, 0, 1, 7, 6}
     *    第二趟： 最坏比较次数 2
     *      {-2, [3], [8], 9, 0, 1, 7, 6}
     *    第三趟： 最坏比较次数 3
     *       {-2, 3, 8, 9, 0, 1, 7, 6}
     *    第四趟： 最坏比较次数 4
     *       {-2, 3, 8, 0, 9, 1, 7, 6}
     *       {-2, 3, 0, [8], [9], 1, 7, 6}
     *       {-2, 0, [3], [8], [9], 1, 7, 6}
     *    第五趟：最坏比较次数 5
     *      {-2, 0, 3, 8, 1, [9], 7, 6}
     *      {-2, 0, 3, 1, [8], [9], 7, 6}
     *      {-2, 0, 1, [3], [8], [9], 7, 6}
     *    第六趟：最坏比较次数 6
     *      {-2, 0, 1, 3, 8, 7, [9], 6}
     *      {-2, 0, 1, 3, 7, [8], [9], 6}
     *    第七趟：最坏比较次数 7
     *      {-2, 0, 1, 3, 7, 8, 6, [9]}
     *      {-2, 0, 1, 3, 7, 6, [8], [9]}
     *      {-2, 0, 1, 3, 6, [7], [8], [9]}
     *     进行比较的次数：
     *      1+2+。。。。+(n-1) = (n - 1) * n / 2 = O(n^2)
     *   ----------------------------------
     *      {-2, 0, 1, 3, 6, 7, 8, 9}
     * @param arr
     */
    public static void insertSort(int[] arr) {
        for (int i = 1; i < arr.length; i++) {
            for (int j = i; j > 0; j--) {
                if(arr[j] < arr[j - 1]) {
                    swap(arr, j, j - 1);
                } else {
                    break;
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