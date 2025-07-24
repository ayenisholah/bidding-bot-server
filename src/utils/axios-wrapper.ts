import axios, { AxiosInstance, AxiosRequestConfig, AxiosResponse, InternalAxiosRequestConfig } from 'axios';
import { timeoutLogger } from './timeout-logger';

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
      const config = response.config as ExtendedAxiosRequestConfig;
      if (config.metadata?.jobId && config.metadata.startTime) {
        const duration = Date.now() - config.metadata.startTime;
        const responseSize = JSON.stringify(response.data).length;
        
        timeoutLogger.logApiCall(
          config.metadata.jobId,
          config.url || '',
          config.method?.toUpperCase() || 'GET',
          config.metadata.startTime,
          response.status,
          responseSize
        );
      }
      return response;
    },
    (error) => {
      const config = error.config as ExtendedAxiosRequestConfig;
      if (config?.metadata?.jobId && config.metadata.startTime) {
        const duration = Date.now() - config.metadata.startTime;
        const status = error.code === 'ECONNABORTED' ? 'timeout' : 'error';
        
        timeoutLogger.logApiCall(
          config.metadata.jobId,
          config.url || '',
          config.method?.toUpperCase() || 'GET',
          config.metadata.startTime,
          status,
          undefined,
          error.message
        );
      }
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