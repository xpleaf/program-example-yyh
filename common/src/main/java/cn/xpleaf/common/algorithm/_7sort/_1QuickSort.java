package cn.xpleaf.common.algorithm._7sort;

import java.util.Arrays;

public class _1QuickSort {

    public static void main(String[] args) {
        int[] arr = {8, -2, 3, 9, 0, 1, 7, 6};
        System.out.println("排序前：" + Arrays.toString(arr));
        quickSort(arr);
        System.out.println("排序后：" + Arrays.toString(arr));
    }

    public static void quickSort(int[] arr) {
        quickSort(arr, 0, arr.length - 1);
    }

    /**
     * 快速排序
     *  是一种分而治之的思想，以某一个基准元素为标准，将集合中比该基准元素小的都放到其左侧，反之放到其右侧。
     *  这样我们就能够找到该基准元素在该集合中所处的位置。
     *  同理，我们就能够找到每一个元素在集合中恰当的位置。
     *  有点类似于二分查找。
     *  一般这个基准元素就是第一个元素。
     *  设置两个指针，一个设置在开头，一个设置末尾，从末尾开始找，找到一个比基本元素小的便停下来，然后
     *  从左侧开始找，直到找到一个比基准元素大的元素，停下来，二者元素进行交换，直到两个指针碰头，本次循环结束
     *  指针指向的位置就是该基准元素在集合中应该所处的位置
     *  eg
     *      {8, -2, 3, 9, 0, 1, 7, 6}
     *  benchmark
     *  第一个bm=8
     *      end = length - 1 = 7
     *      start=0
     *      end--,我们发现6比8小，end指针停下来了，当前索引为j=7
     *      start++,直到元素为9的位置停下来,当前索引i=3
     *      将i和j所对应的元素进行交换
     *    {8, -2, 3, [6], 0, 1, 7, [9]}
     *                i             j
     * 循环继续
     *    end--,到索引为6的位置又停下来了
     *    start++,直到和end碰头都没有再找到比8大的元素，所以我们就能断定，碰头的这个位置就应该是8在该集合中应该在的位置
     *   交换8的索引和碰头的索引
     *   {8, -2, 3, 6, 0, 1, 7, 9}
     *                       i=j=6
     * 交换：{7, -2, 3, 6, 0, 1, 8, 9}
     * 同理，我们可以在8左侧重复上述操作，8的右侧也可以重复上述操作
     * 使用递归调用的方式来完成集合的排序
     * @param arr
     */
    public static void quickSort(int[] arr, int low, int high) {
        if(low > high) {
            return;
        }
        // 默认以[low, high]中的arr[low]作为基准值
        int index = arr[low];
        // 定义左指针
        int start = low;
        // 定义右指针
        int end = high;
        // 开始向基准值扫描，当start < end条件不满足时
        // 说明指针碰头，则需要将基准值与start进行交换
        // 即该基准值在整个元素集合中的位置已经确定
        // 其左边的值比基准值小，其右边的值比基准值大
        while (start < end) {
            // 按照前面算法的设计，先从右边开始扫描，直到找到比基准值小的数再停下来
            // （下面循环的意义就是，在start < end的前提下，如果end位置的值比index值大或等于，就继续往左边找）
            while (start < end && arr[end] >= index) {
                end--;
            }
            // 财从左边开始扫描，直到找到比基准值大的数再停下来
            // （下面循环的意义就是，在start < end的前提下，如果start位置的值比index值小或等于，就继续往右边找）
            while (start < end && arr[start] <= index) {
                start++;
            }
            if (start < end) {
                // 前面的循环结束后，如果start还是小于end
                // 那么就交换arr[start]和arr[end]的值
                swap(arr, start, end);
            }
        }
        // 上面的循环结束后，start指针和end指针碰头，交换arr[start]和基准值
        arr[low] = arr[start];
        arr[start] = index;
        // 交换后，arr[start]左边比它小，右边比它大，然后再以它为基准
        // 左边和右边进行相同的操作
        quickSort(arr, low, start - 1);
        quickSort(arr, start + 1, high);
    }

    /**
     * 位操作
     * 按位与(&)
     * 1&1=1
     * 1&0=0
     * 0&1=0
     * 0&0=0
     * 异或(^)
     * （值不同）就取真（1），否则为（0）
     * 1&1=0
     * 1&0=1
     * 0&1=1
     * 0&0=0
     * 非
     *
     * @param arr
     * @param i
     * @param j
     */
    private static void swap(int[] arr, int i, int j) {
        /*int tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;*/
        arr[i] = arr[i] ^ arr[j];
        arr[j] = arr[i] ^ arr[j];
        arr[i] = arr[i] ^ arr[j];
    }

}

/*
交换a=3和b=5，不用第三方变量，效率最高
    方法一：
        a = a + b = 8
        b = a - b = (8 - 5) = 3
        a = a - b = (8 - 3) = 5
异或的方式
    a=3-->低8为0000 0011
    b=5-->低8为0000 0101

    a = a ^ b
         0000 0011
       ^ 0000 0101
       ---------------
         0000 0110 --->6
    b = a ^ b
         0000 0110
       ^ 0000 0101
       --------------
         0000 0011--->3
    a = a ^ b
         0000 0110
       ^ 0000 0011
      ---------------
         0000 0101--->5
*/

