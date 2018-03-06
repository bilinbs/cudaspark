
import java.util.ArrayList;
import java.util.List;

import static jcuda.driver.JCudaDriver.cuCtxCreate;
import static jcuda.driver.JCudaDriver.cuCtxSynchronize;
import static jcuda.driver.JCudaDriver.cuDeviceGet;
import static jcuda.driver.JCudaDriver.cuInit;
import static jcuda.driver.JCudaDriver.cuMemAlloc;
import static jcuda.driver.JCudaDriver.cuMemFree;
import static jcuda.driver.JCudaDriver.cuMemcpyDtoH;
import static jcuda.driver.JCudaDriver.cuMemcpyHtoD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import jcuda.Pointer;
import jcuda.Sizeof;
import jcuda.driver.CUcontext;
import jcuda.driver.CUdevice;
import jcuda.driver.CUdeviceptr;
import jcuda.driver.JCudaDriver;
import jcuda.utils.KernelLauncher;

public class SimpleApp {
    static int NUM_SAMPLES = 4;
    static String sourceCode = "extern \"C\"\n" + 
            "__global__ void add(int n, float *a, float *b, float *sum)\n" + 
            "{\n" + 
            "    int i = blockIdx.x * blockDim.x + threadIdx.x;\n" + 
            "    if (i<n)\n" + 
            "    {\n" + 
            "        sum[i] = a[i] + b[i];\n" + 
            "    }\n" + 
            "\n" + 
            "}";
    public static void main(String[] args) {
     
        
        
        SparkConf conf = new SparkConf().setAppName("simpleapp").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> l = new ArrayList<>(NUM_SAMPLES);
        for (int i = 0; i < NUM_SAMPLES; i++) {
            l.add(i);
        }
        JavaRDD<FloatArr> res = sc.parallelize(l, 4).map(i -> {
            //double x = Math.random();
            //double y = Math.random();
            return cudaArrAdd();
        });
        res.saveAsTextFile("output/cuspark1");
        //System.out.println("Pi is roughly " + 4.0 * count / NUM_SAMPLES);
        
    }
    
    public static FloatArr cudaArrAdd() {
     // Enable exceptions and omit all subsequent error checks
        JCudaDriver.setExceptionsEnabled(true);
     // Initialize the driver and create a context for the first device.
        cuInit(0);
        CUdevice device = new CUdevice();
        cuDeviceGet(device, 0);
        CUcontext cuContext = new CUcontext();
        cuCtxCreate(cuContext, 0, device);
        KernelLauncher kernelLauncher = KernelLauncher.compile(sourceCode, "add");
        
        int numElements = 1000000;

        // Allocate and fill the host input data
        float hostInputA[] = new float[numElements];
        float hostInputB[] = new float[numElements];
        for(int i = 0; i < numElements; i++)
        {
            hostInputA[i] = (float)i;
            hostInputB[i] = (float)(numElements - i);
        }
        
     // Allocate the device input data, and copy the
        // host input data to the device
        CUdeviceptr deviceInputA = new CUdeviceptr();
        cuMemAlloc(deviceInputA, numElements * Sizeof.FLOAT);
        cuMemcpyHtoD(deviceInputA, Pointer.to(hostInputA),
            numElements * Sizeof.FLOAT);
        CUdeviceptr deviceInputB = new CUdeviceptr();
        cuMemAlloc(deviceInputB, numElements * Sizeof.FLOAT);
        cuMemcpyHtoD(deviceInputB, Pointer.to(hostInputB),
            numElements * Sizeof.FLOAT);

        // Allocate device output memory
        CUdeviceptr deviceOutput = new CUdeviceptr();
        cuMemAlloc(deviceOutput, numElements * Sizeof.FLOAT);
        
        int blockSizeX = 512;
        int gridSizeX = (int)Math.ceil((double)numElements / blockSizeX);
        
        kernelLauncher.setBlockSize(blockSizeX, 1, 1);
        kernelLauncher.setGridSize(gridSizeX, 1, 1);
        kernelLauncher.call(numElements, deviceInputA, deviceInputB, deviceOutput);
        cuCtxSynchronize();

        // Allocate host output memory and copy the device output
        // to the host.
        float hostOutput[] = new float[numElements];
        cuMemcpyDtoH(Pointer.to(hostOutput), deviceOutput,
            numElements * Sizeof.FLOAT);
        //
     // Clean up.
        cuMemFree(deviceInputA);
        cuMemFree(deviceInputB);
        cuMemFree(deviceOutput);
        
        return new FloatArr(hostOutput);
    }
}

class FloatArr{
    float [] array;
    
    FloatArr(float[] array){
        this.array = array;
    }
    
    public String toString() {
        StringBuffer sb = new StringBuffer();
        for(float f: array) {
            sb.append(f);
            sb.append("\n");
        }
        return sb.toString();
    }
}
