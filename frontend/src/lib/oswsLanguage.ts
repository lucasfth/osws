import { StreamLanguage } from "@codemirror/language"

const ACTION_KW = /^(grant|revoke|create|drop)\b/i
const STRUCT_KW = /^(role|access|on|to|from)\b/i

export const oswsLanguage = StreamLanguage.define({
  token(stream) {
    if (stream.match(/--.*$/)) return "comment"
    if (stream.match(ACTION_KW)) return "keyword"
    if (stream.match(STRUCT_KW)) return "type"
    if (stream.match(/[a-zA-Z0-9_.@-]+/)) return "variable"
    if (stream.match(/;/)) return "punctuation"
    stream.next()
    return null
  },
  languageData: { commentTokens: { line: "--" } },
})
