import { Controller, Get } from '@nestjs/common';

@Controller()
export class AppController {
  @Get()
  getStatus(): { name: string; status: string } {
    return { name: 'queue-system', status: 'ok' };
  }
}
