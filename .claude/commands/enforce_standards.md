# 코드 표준 검사

변경된 파일에 대해 코딩 표준 준수 여부를 검사합니다.

## 실행 단계

### Step 1: Ruff 포매팅/린트 실행

```bash
ruff check --fix .
ruff format .
```

ruff에서 자동 수정되지 않는 문제가 있으면 수동으로 수정합니다.

### Step 2: 코드 표준 검사 에이전트 실행

Task 도구를 사용하여 `code-standards-enforcer` 에이전트를 실행합니다.

**에이전트 유형**: `code-standards-enforcer`
**설명**: "변경 파일 코드 표준 검사"

**프롬프트**:
```
현재 브랜치에서 변경된 모든 파일에 대해 `.claude/CLAUDE.md`의 코딩 컨벤션 준수 여부를 검사하세요.

검사 항목:
1. 타입 어노테이션 (빌트인 타입 사용, typing.Dict/List 금지)
2. Import 규칙 (모듈 최상위 import)
3. 예외 처리 패턴 (예외를 제어 흐름으로 사용 금지)
4. Pydantic 모델 사용 (BaseModel, Field)
5. Dagster 로깅 (print 금지, context.log 사용)
6. Context Manager 사용 패턴
7. ruff 포매팅/린트 통과 여부

검사 후 제공:
- 위반 사항 요약
- 파일별 구체적인 수정 권장
- 표준 준수 확인
```

## 오류 처리

ruff 실행이 실패하면:
1. 실행을 즉시 중단
2. 오류 내용을 사용자에게 보고
3. 수동 수정 가이드 제공

## 성공 기준

1. `ruff check .` 및 `ruff format --check .`이 오류 없이 통과
2. 코드 표준 검사 에이전트가 위반 사항 없이 완료
3. 발견된 위반 사항이 모두 수정됨
