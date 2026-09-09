import { Injectable } from '@nestjs/common';

@Injectable()
export class AppService {
  getStatus(): { name: string; status: string } {
    return { name: 'queue-system', status: 'ok' };
  }
}
