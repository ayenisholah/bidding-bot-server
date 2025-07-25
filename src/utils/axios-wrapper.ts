import { AxiosInstance, AxiosResponse, InternalAxiosRequestConfig } from 'axios';
interface ExtendedAxiosRequestConfig extends InternalAxiosRequestConfig {
  metadata?: {
    startTime: number;
    jobId?: string;
  };
}

export function wrapAxiosInstance(instance: AxiosInstance, jobIdGetter: () => string | undefined): void {
  // Request interceptor
  instance.interceptors.request.use(
    (config: InternalAxiosRequestConfig) => {
      const extendedConfig = config as ExtendedAxiosRequestConfig;
      extendedConfig.metadata = {
        startTime: Date.now(),
        jobId: jobIdGetter()
      };

      if (extendedConfig.metadata.jobId) {
        const url = extendedConfig.url || '';
        const method = extendedConfig.method?.toUpperCase() || 'GET';
        console.log(`[API Request] Job ${extendedConfig.metadata.jobId} - ${method} ${url}`);
      }

      return extendedConfig;
    },
    (error) => {
      return Promise.reject(error);
    }
  );

  // Response interceptor
  instance.interceptors.response.use(
    (response: AxiosResponse) => {
      return response;
    },
    (error) => {
      return Promise.reject(error);
    }
  );
}

// Create a context storage for current job ID
class JobContext {
  private currentJobId: string | undefined;

  setJobId(jobId: string | undefined) {
    this.currentJobId = jobId;
  }

  getJobId(): string | undefined {
    return this.currentJobId;
  }
}

export const jobContext = new JobContext();