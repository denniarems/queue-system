import { Controller, Get } from '@nestjs/common';
import { ApiOperation, ApiResponse, ApiTags } from '@nestjs/swagger';
import { HealthResponseDto } from './payments/dto/payment-response.dto.js';

@ApiTags('Health')
@Controller()
export class AppController {
  @Get()
  @ApiOperation({
    summary: 'System health check',
    description: 'Basic health check endpoint returning system status.',
  })
  @ApiResponse({
    status: 200,
    description: 'System operational status',
    type: HealthResponseDto,
  })
  getStatus(): HealthResponseDto {
    return { name: 'queue-system', status: 'ok' };
  }
}
